//! Starts a multipart upload, gating placement and pinning the backend on the upload record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::distribution::guard::write_authority;
use crate::distribution::guard::{GuardError, WriteAuthority, authority_reads, check_authority};
use crate::distribution::inventory::{
    InventoryChange, InventoryError, InventoryUpdate, StagedCall, outside_answer,
};
use crate::groups::backends::{BackendFenceError, backend_key};
use crate::placement::policy::read::ReadPolicyError;
use crate::placement::policy::{
    GateContext, GatedBucket, PolicyGateError, PolicyGateOutcome, gate_decision, write_gate,
};
use crate::s3::object::put::abe::{abe_reads, keyed, parse_abe};
use crate::s3::object::put::{owner_call, path_owned, same_name_path};
use crate::s3::purge_fence::{PurgeFenceError, check_fence_row, fence_key};
use crate::s3::usage::upload_count;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{
    ABE_PENDING_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE, PURGE_FENCE_KEYSPACE,
    S3_BUCKET_KEYSPACE, STORAGE_BACKEND_KEYSPACE, UPLOAD_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::abe::{AbeEffect, AbeEvent, EnvelopePlan, ObjectEnvelope};
use aruna_core::structs::storage::backend_import::PlainPath;
use aruna_core::structs::storage::blob::{BackendRef, BucketInfo, ResolvedBackend};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyError, BucketKeyRecord, SealPlan,
};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::structs::storage::multipart::{
    BackendUpload, MultipartChecksumHint, MultipartChecksumType, MultipartUpload,
    MultipartUploadStatus, UploadEncryption,
};
use aruna_core::structs::storage::routing::{RoutingError, RoutingSnapshot, resolve_backend};
use aruna_core::types::{GroupId, Key, Value};
use std::collections::HashMap;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateMultipartState {
    ReadGateBucket,
    ReadBucketKey,
    PolicyGate,
    /// Reads a distributed bucket's record: uploads open only on the key's coordinator.
    ReadRecord,
    CheckOpenFence,
    ReadPlainOwner,
    StatPlain,
    OpenUpload,
    ReadAbe,
    CreateEnvelope,
    CheckPurgeFence,
    /// Rereads the binding, record and fence of a distributed bucket in the transaction.
    CheckAuthority,
    FenceBackend,
    CheckSettings,
    WriteUpload,
    /// Books the new record on the bucket's open upload count.
    CountUpload,
    /// Opens the upload in the upload inventory of a distributed bucket.
    CountInventory,
    WritePending,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateMultipartError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error("{}", aruna_core::structs::storage::routing::UNMAPPED_KEY)]
    Unmapped,
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    Distribution(#[from] GuardError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received}]")]
    InvalidStateEvent {
        state: CreateMultipartState,
        expected: &'static str,
        received: &'static str,
    },
    #[error(transparent)]
    RoutingFailed(#[from] RoutingError),
    #[error(transparent)]
    BackendFenceError(#[from] BackendFenceError),
    #[error(transparent)]
    PolicyGateError(#[from] PolicyGateError),
    #[error(transparent)]
    PurgeFence(#[from] PurgeFenceError),
    #[error("CreateMultipartUpload failed")]
    CreateUploadFailed,
    #[error(transparent)]
    BlobError(#[from] BlobError),
    #[error(transparent)]
    BucketKey(#[from] BucketKeyError),
    #[error("an encrypted upload cannot check a full-object {0} checksum")]
    UnsupportedChecksum(&'static str),
    #[error(transparent)]
    Inventory(#[from] InventoryError),
}

impl From<IoError> for CreateMultipartError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

type Turn = Result<Next<CreateMultipartResult>, CreateMultipartError>;

/// A read before the transaction, which the upload record's transaction checks again.
fn outside(call: IoCall) -> IoCall {
    StagedCall::Outside(call).into_call()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateMultipartInput {
    pub bucket: String,
    pub key: String,
    pub group_id: GroupId,
    pub created_by: UserId,
    pub checksum_hint: Option<MultipartChecksumHint>,
    /// Routing inputs; the resolved backend is pinned on the upload record so
    /// every part and the composed object follow it.
    pub routing: RoutingSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateMultipartResult {
    pub record: MultipartUpload,
}

#[derive(Debug, PartialEq)]
pub struct CreateMultipartOperation {
    input: CreateMultipartInput,
    /// Chosen before the provider upload opens, so its cleanup row can name the record.
    upload_id: Ulid,
    state: CreateMultipartState,
    resolved: Option<ResolvedBackend>,
    record: Option<MultipartUpload>,
    metadata: HashMap<String, String>,
    /// Destination details of this node. Absent fails every governed upload
    /// closed and leaves the ungoverned path untouched.
    gate_context: Option<GateContext>,
    /// Refs and subject the gate admitted, stored on the upload record so every
    /// part and the completion inherit exactly what was evaluated here.
    stored_policies: Vec<PlacementPolicyRef>,
    stored_subject: u64,
    /// The provider upload opened for this record; aborted unless a commit may own it.
    backend_upload: Option<BackendUpload>,
    /// The commit was sent: a failure from here on may still have committed the record.
    committing: bool,
    /// The plain path and version id a same-name upload checks before it opens.
    plain: Option<PlainPath>,
    /// The file at the plain path may be replaced: the rule recorded it for this key.
    plain_owned: bool,
    /// Bucket and settings read before the gate, kept until the key record completes the plan.
    settings: Option<(Option<BucketInfo>, BucketEncryption)>,
    encryption: Option<UploadEncryption>,
    /// The pending envelope of an encrypted upload, stored beside its record.
    envelope: Option<ObjectEnvelope>,
    /// The inventory update of a distributed bucket, opened with the upload record.
    inventory: Option<InventoryUpdate>,
    /// The bucket's distribution binding; only the key's coordinator opens an upload.
    binding: Option<aruna_core::structs::storage::distribution::DistributionBinding>,
    /// The coordinator authority read before the transaction, checked again inside it.
    authority: Option<WriteAuthority>,
}

impl CreateMultipartOperation {
    pub fn new(input: CreateMultipartInput) -> Self {
        Self {
            input,
            upload_id: Ulid::generate(),
            state: CreateMultipartState::ReadGateBucket,
            resolved: None,
            record: None,
            metadata: HashMap::new(),
            gate_context: None,
            stored_policies: Vec::new(),
            stored_subject: 0,
            backend_upload: None,
            committing: false,
            plain: None,
            plain_owned: false,
            settings: None,
            encryption: None,
            envelope: None,
            inventory: None,
            binding: None,
            authority: None,
        }
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// The destination this upload is evaluated against. Omitting it leaves the
    /// ungoverned path untouched and fails every governed upload closed.
    pub fn with_gate(mut self, context: GateContext) -> Self {
        self.gate_context = Some(context);
        self
    }

    fn call(&mut self, state: CreateMultipartState, call: IoCall) -> Turn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn unexpected(&self, expected: &'static str, event: Event) -> CreateMultipartError {
        if let Event::Blob(BlobEvent::Error(error)) = event {
            return error.into();
        }
        CreateMultipartError::InvalidStateEvent {
            state: self.state.clone(),
            expected,
            received: event.label(),
        }
    }

    fn resolved(&self) -> Result<ResolvedBackend, CreateMultipartError> {
        self.resolved
            .clone()
            .ok_or(CreateMultipartError::CreateUploadFailed)
    }

    /// A fenced destination opens no provider upload; the transaction checks the fence again.
    fn check_open_fence(&mut self) -> Turn {
        if let Some(binding) = self.binding.as_ref() {
            let reads = authority_reads(binding.set_id);
            return self.call(
                CreateMultipartState::ReadRecord,
                outside(IoCall::Read(reads)),
            );
        }
        self.read_open_fence()
    }

    fn record_checked(&mut self, values: Vec<Option<Value>>) -> Turn {
        let binding = self.binding.as_ref();
        let binding = binding.ok_or(CreateMultipartError::CreateUploadFailed)?;
        let [record, fence] =
            <[_; 2]>::try_from(values).map_err(|_| CreateMultipartError::CreateUploadFailed)?;
        let key = &self.input.key;
        let authority = write_authority(binding, key, record.as_deref(), fence.as_deref())?;
        self.authority = Some(authority);
        self.read_open_fence()
    }

    fn read_open_fence(&mut self) -> Turn {
        let read = read_row(PURGE_FENCE_KEYSPACE, fence_key(&self.input.bucket));
        self.call(CreateMultipartState::CheckOpenFence, outside(read))
    }

    /// S3 backends open the provider upload the parts stream into; others answer `None`.
    fn open_upload(&mut self, fence: Option<Value>) -> Turn {
        check_fence_row(fence.as_deref(), &self.input.bucket, &self.input.key)?;
        // Encrypted parts are sealed pieces of their own; completion composes them.
        if let Some(encryption) = self.encryption {
            let reads = abe_reads(encryption.plan.key);
            return self.call(CreateMultipartState::ReadAbe, outside(IoCall::Read(reads)));
        }
        let resolved = self.resolved()?;
        // Under an import rule the provider upload ends at the plain path its version id names.
        let (bucket, key, id) = (&self.input.bucket, &self.input.key, Ulid::generate());
        // A folder object holds no bytes, so no upload can compose one.
        if key.ends_with('/') && self.input.routing.mapped(bucket) {
            return Err(CreateMultipartError::Unmapped);
        }
        let Some(target) = self
            .input
            .routing
            .plain_target(bucket, key, id)
            .filter(|target| target.backend == resolved.backend)
        else {
            if self.input.routing.mapped(bucket) {
                return Err(CreateMultipartError::Unmapped);
            }
            return self.open_native(None);
        };
        let plain = PlainPath {
            path: target.path.clone(),
            ulid: id,
            version_id: id,
            if_not_exists: false,
            exact: false,
        };
        if !target.same_name {
            return self.open_native(Some(plain));
        }
        // A same-name upload picks its path as a single write does; completion checks it again.
        self.plain = Some(plain);
        match owner_call(&target) {
            Some(read) => self.call(CreateMultipartState::ReadPlainOwner, outside(read)),
            None => {
                // As at completion, only a record proves the path the upload's own.
                self.plain_owned = false;
                self.stat_plain()
            }
        }
    }

    fn plain_owner(&mut self, value: Option<Value>) -> Turn {
        self.plain_owned = path_owned(value.as_deref(), &self.input.key)?;
        self.stat_plain()
    }

    fn stat_plain(&mut self) -> Turn {
        let plain = self.plain.as_ref();
        let path = plain
            .ok_or(CreateMultipartError::CreateUploadFailed)?
            .path
            .clone();
        let stat = BlobEffect::StatPlain {
            backend: self.resolved()?.backend,
            path,
        };
        self.call(CreateMultipartState::StatPlain, Effect::Blob(stat).into())
    }

    fn plain_stat(&mut self, event: Event) -> Turn {
        let plain = self.plain.take();
        let plain = plain.ok_or(CreateMultipartError::CreateUploadFailed)?;
        let Event::Blob(BlobEvent::PlainStat { exists, version }) = event else {
            return Err(self.unexpected("Event::Blob(BlobEvent::PlainStat)", event));
        };
        let replaces = version.is_some() && self.plain_owned;
        let (path, if_not_exists) = same_name_path(&plain.path, plain.ulid, exists, replaces);
        self.open_native(Some(PlainPath {
            path,
            if_not_exists,
            ..plain
        }))
    }

    /// Backends without a provider upload answer `None`; their parts are composed instead.
    fn open_native(&mut self, plain: Option<PlainPath>) -> Turn {
        let open = BlobEffect::OpenUpload {
            record_id: self.upload_id,
            bucket: self.input.bucket.clone(),
            key: self.input.key.clone(),
            resolved: self.resolved()?,
            created_by: self.input.created_by,
            plain,
        };
        self.call(CreateMultipartState::OpenUpload, Effect::Blob(open).into())
    }

    fn upload_opened(&mut self, event: Event) -> Turn {
        let Event::Blob(BlobEvent::UploadOpened { backend_upload }) = event else {
            return Err(self.unexpected("Event::Blob(BlobEvent::UploadOpened)", event));
        };
        self.backend_upload = backend_upload;
        self.check_purge_fence()
    }

    /// Creates the object key and envelope now, so completion needs no private key.
    fn abe_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Turn {
        let plan = self.encryption.map(|encryption| encryption.plan);
        let plan = plan.ok_or(CreateMultipartError::CreateUploadFailed)?;
        // A generation without admitted parameters keeps bucket-only pieces.
        if (values.first()).is_some_and(|(_, parameters)| parameters.is_none()) {
            return self.check_purge_fence();
        }
        let (parameters, epoch) =
            parse_abe(&values, plan.key).map_err(|error| BlobError::from(error))?;
        let envelope = AbeEffect::Envelope(EnvelopePlan {
            parameters,
            epoch,
            write_id: Ulid::generate(),
            object_key: self.input.key.clone(),
            bucket_public: plan.public_key,
        });
        let create = Effect::Blob(BlobEffect::Abe(Box::new(envelope)));
        self.call(CreateMultipartState::CreateEnvelope, create.into())
    }

    fn envelope_created(&mut self, event: Event) -> Turn {
        let Event::Blob(BlobEvent::Abe(abe)) = event else {
            return Err(self.unexpected("Event::Blob(BlobEvent::Abe)", event));
        };
        let AbeEvent::Envelope(envelope) = *abe else {
            return Err(CreateMultipartError::CreateUploadFailed);
        };
        self.envelope = Some(envelope);
        self.check_purge_fence()
    }

    /// Reads the bucket and its encryption settings; an encrypted bucket also needs its key.
    fn handle_gate_bucket(&mut self, values: Vec<Option<Value>>) -> Turn {
        let [bucket, settings] =
            <[_; 2]>::try_from(values).map_err(|_| CreateMultipartError::CreateUploadFailed)?;
        let bucket = bucket
            .as_ref()
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?;
        let settings = BucketEncryption::from_row(settings.as_deref())?;
        let Some(key) = settings.active_key() else {
            return self.gate_bucket(bucket);
        };
        self.settings = Some((bucket, settings));
        let read = read_row(BUCKET_KEY_KEYSPACE, key.key());
        self.call(CreateMultipartState::ReadBucketKey, outside(read))
    }

    /// Captures the seal plan from the active key record, then gates the bucket.
    fn handle_bucket_key(&mut self, value: Option<Value>) -> Turn {
        let settings = self.settings.take();
        let (bucket, settings) = settings.ok_or(CreateMultipartError::CreateUploadFailed)?;
        let value = value.ok_or(BucketKeyError::Unsupported)?;
        let plan = BucketKeyRecord::from_bytes(value.as_ref())
            .map_err(CreateMultipartError::from)
            .and_then(|record| Ok(SealPlan::capture(&settings, &record)?));
        if let Some(algorithm) = self.input.checksum_hint.as_ref().and_then(full_digest) {
            return Err(CreateMultipartError::UnsupportedChecksum(
                algorithm.s3_name(),
            ));
        }
        let plan = plan?.ok_or(BucketKeyError::Unsupported)?;
        let compression = bucket.as_ref().map(|bucket| bucket.compression);
        self.encryption = Some(UploadEncryption {
            plan,
            compression: compression.unwrap_or_default(),
        });
        self.gate_bucket(bucket)
    }

    fn gate_bucket(&mut self, bucket: Option<BucketInfo>) -> Turn {
        self.stored_policies = GatedBucket::observe(bucket.as_ref()).policies;
        self.binding = bucket
            .as_ref()
            .and_then(|bucket| bucket.distribution.clone());
        self.inventory = self.binding.as_ref().map(|binding| {
            let mut change = InventoryChange::new(binding.set_id, self.input.key.clone());
            change.upload = Some((self.upload_id, true));
            InventoryUpdate::new(change)
        });
        let compression = bucket.as_ref().map(|bucket| bucket.compression);
        self.resolved = self
            .resolved
            .take()
            .map(|resolved| resolved.with_compression(compression.unwrap_or_default()));
        let group_id = bucket
            .as_ref()
            .map_or(self.input.group_id, |bucket| bucket.group_id);
        let gate = write_gate(
            self.gate_context.as_ref(),
            &self.stored_policies,
            Some(group_id),
        )?;
        match gate {
            None => self.check_open_fence(),
            Some(gate) => self.call(CreateMultipartState::PolicyGate, IoCall::child(gate)),
        }
    }

    fn finish_gate(&mut self, result: IoResult) -> Turn {
        let outcome = result.child::<PolicyGateOutcome, ReadPolicyError>()?;
        gate_decision(outcome.map_err(PolicyGateError::from)?)?;
        self.stored_subject = self
            .gate_context
            .as_ref()
            .map_or(0, |context| context.subject.generation);
        self.check_open_fence()
    }

    /// The first read of the transaction, which opens it.
    fn check_purge_fence(&mut self) -> Turn {
        let read = read_row(PURGE_FENCE_KEYSPACE, fence_key(&self.input.bucket));
        self.call(CreateMultipartState::CheckPurgeFence, read)
    }

    fn fence_checked(&mut self, fence: Option<Value>) -> Turn {
        check_fence_row(fence.as_deref(), &self.input.bucket, &self.input.key)?;
        let Some(authority) = self.authority.as_ref() else {
            return self.fence_backend();
        };
        // A fence, transfer or replaced bucket since the first check conflicts the commit.
        let bucket = (
            S3_BUCKET_KEYSPACE.to_string(),
            self.input.bucket.as_bytes().into(),
        );
        let mut reads = vec![bucket];
        reads.extend(authority_reads(authority.binding.set_id));
        self.call(CreateMultipartState::CheckAuthority, IoCall::Read(reads))
    }

    fn authority_checked(&mut self, values: Vec<Option<Value>>) -> Turn {
        let authority = self.authority.as_ref();
        let authority = authority.ok_or(CreateMultipartError::CreateUploadFailed)?;
        let mut values = values.into_iter();
        let (bucket, record, fence) = (
            values.next().flatten(),
            values.next().flatten(),
            values.next().flatten(),
        );
        let bound = (bucket.as_deref())
            .and_then(|value| BucketInfo::from_bytes(value).ok())
            .and_then(|info| info.distribution)
            .is_some_and(|bound| {
                let wanted = &authority.binding;
                (bound.set_id, bound.member_id, bound.incarnation)
                    == (wanted.set_id, wanted.member_id, wanted.incarnation)
            });
        if !bound {
            return Err(GuardError::Unbound(authority.binding.set_id).into());
        }
        let key = &self.input.key;
        check_authority(authority, key, record.as_deref(), fence.as_deref())?;
        self.fence_backend()
    }

    /// A group backend must still exist and be enabled when the record commits.
    fn fence_backend(&mut self) -> Turn {
        let BackendRef::Group(backend_id) = self.resolved()?.backend else {
            return self.check_settings();
        };
        let read = read_row(STORAGE_BACKEND_KEYSPACE, backend_key(backend_id));
        self.call(CreateMultipartState::FenceBackend, read)
    }

    fn backend_fenced(&mut self, value: Option<Value>) -> Turn {
        let record = value
            .map(|value| GroupStorage::from_bytes(&value))
            .transpose()?;
        if !record.is_some_and(|record| !record.disabled) {
            return Err(BackendFenceError::Unavailable.into());
        }
        self.check_settings()
    }

    /// Rereads the settings in the transaction, so a mode change or rotation that started
    /// meanwhile conflicts instead of leaving an upload with a stale plan.
    fn check_settings(&mut self) -> Turn {
        let read = read_row(
            BUCKET_ENCRYPTION_KEYSPACE,
            self.input.bucket.as_bytes().to_vec(),
        );
        self.call(CreateMultipartState::CheckSettings, read)
    }

    fn settings_checked(&mut self, value: Option<Value>) -> Turn {
        let plan = self.encryption.map(|encryption| encryption.plan);
        let settings = BucketEncryption::from_row(value.as_deref())?;
        storage_current(plan.as_ref(), &settings)?;
        self.write_upload()
    }

    fn write_upload(&mut self) -> Turn {
        let resolved = self.resolved.take();
        let resolved = resolved.ok_or(CreateMultipartError::CreateUploadFailed)?;
        let record = MultipartUpload {
            backend: resolved.backend,
            storage_class: resolved.storage_class,
            upload_id: self.upload_id,
            bucket: self.input.bucket.clone(),
            key: self.input.key.clone(),
            group_id: self.input.group_id,
            created_by: self.input.created_by,
            created_at: SystemTime::now(),
            status: MultipartUploadStatus::Open,
            checksum_hint: self.input.checksum_hint.clone(),
            metadata: self.metadata.clone(),
            placement_policies: self.stored_policies.clone(),
            subject_generation: self.stored_subject,
            completing_since_ms: None,
            backend_upload: self.backend_upload.clone(),
            encryption: self.encryption,
        };
        let row = (
            UPLOAD_KEYSPACE.to_string(),
            record.upload_id.to_bytes().to_vec().into(),
            record.to_bytes()?.into(),
        );
        self.record = Some(record);
        self.call(CreateMultipartState::WriteUpload, IoCall::Write(vec![row]))
    }

    /// The bucket counts the upload in the transaction that creates its record.
    fn record_written(&mut self) -> Turn {
        let count = upload_count(&self.input.bucket, 1);
        self.call(CreateMultipartState::CountUpload, count)
    }

    fn upload_counted(&mut self) -> Turn {
        let call = self
            .inventory
            .as_mut()
            .and_then(InventoryUpdate::start_calls);
        match call {
            Some(call) => self.call(CreateMultipartState::CountInventory, call.into_call()),
            None => self.write_pending(),
        }
    }

    fn inventory_counted(&mut self, result: IoResult) -> Turn {
        let inventory = self.inventory.as_mut();
        let inventory = inventory.ok_or(CreateMultipartError::CreateUploadFailed)?;
        match inventory.step_calls(result)? {
            Some(call) => Ok(Next::Call(call.into_call())),
            None => self.write_pending(),
        }
    }

    /// Writes the pending envelope, if any, then commits.
    fn write_pending(&mut self) -> Turn {
        if let Some(envelope) = self.envelope.take() {
            let value = envelope.to_bytes().map_err(BlobError::from)?;
            let row = (
                ABE_PENDING_KEYSPACE.to_string(),
                self.upload_id.to_bytes().to_vec().into(),
                value.into(),
            );
            return self.call(CreateMultipartState::WritePending, IoCall::Write(vec![row]));
        }
        self.created()
    }

    /// The committed record owns the provider upload from here on.
    fn created(&mut self) -> Turn {
        let record = self.record.clone();
        let record = record.ok_or(CreateMultipartError::CreateUploadFailed)?;
        self.committing = true;
        Ok(Next::Done(CreateMultipartResult { record }))
    }
}

/// A plain upload needs a bucket that still stores plain bytes; a sealed one its exact plan.
pub(crate) fn storage_current(
    plan: Option<&SealPlan>,
    settings: &BucketEncryption,
) -> Result<(), BucketKeyError> {
    match plan {
        Some(plan) => plan.still_current(settings),
        None if settings.is_encrypted() => Err(BucketKeyError::StaleGeneration {
            requested: 0,
            current: settings.key_generation,
        }),
        None => Ok(()),
    }
}

/// Sealed parts are combined without their plaintext, so only CRCs give a full-object checksum.
fn full_digest(hint: &MultipartChecksumHint) -> Option<ChecksumAlgorithm> {
    let algorithm = hint.algorithm?;
    let crc = matches!(
        algorithm,
        ChecksumAlgorithm::Crc32 | ChecksumAlgorithm::Crc32c | ChecksumAlgorithm::Crc64Nvme
    );
    (hint.checksum_type == MultipartChecksumType::FullObject && !crc).then_some(algorithm)
}

impl Operation for CreateMultipartOperation {
    type Output = CreateMultipartResult;
    type Error = CreateMultipartError;

    fn first(&mut self) -> Turn {
        // Routing is fallible, so it resolves before a transaction exists to abort.
        let routing = &self.input.routing;
        self.resolved = Some(resolve_backend(
            routing,
            &self.input.bucket,
            &self.input.key,
        )?);
        // The destination default is read before the upload exists, so no part
        // can ever be written under a rule this node was never admitted for.
        let key: Key = self.input.bucket.as_bytes().to_vec().into();
        let reads = vec![
            (S3_BUCKET_KEYSPACE.to_string(), key.clone()),
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), key),
        ];
        self.call(
            CreateMultipartState::ReadGateBucket,
            outside(IoCall::Read(reads)),
        )
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.state {
            CreateMultipartState::ReadGateBucket => {
                self.handle_gate_bucket(outside_answer(result)?.values()?)
            }
            CreateMultipartState::ReadBucketKey => {
                self.handle_bucket_key(outside_answer(result)?.value()?)
            }
            CreateMultipartState::PolicyGate => self.finish_gate(result),
            CreateMultipartState::ReadRecord => {
                self.record_checked(outside_answer(result)?.values()?)
            }
            CreateMultipartState::CheckOpenFence => {
                self.open_upload(outside_answer(result)?.value()?)
            }
            CreateMultipartState::ReadPlainOwner => {
                self.plain_owner(outside_answer(result)?.value()?)
            }
            CreateMultipartState::StatPlain => self.plain_stat(result.event()?),
            CreateMultipartState::OpenUpload => self.upload_opened(result.event()?),
            CreateMultipartState::ReadAbe => {
                let values = outside_answer(result)?.values()?;
                self.abe_read(keyed(&values))
            }
            CreateMultipartState::CreateEnvelope => self.envelope_created(result.event()?),
            CreateMultipartState::CheckPurgeFence => self.fence_checked(result.value()?),
            CreateMultipartState::CheckAuthority => self.authority_checked(result.values()?),
            CreateMultipartState::FenceBackend => self.backend_fenced(result.value()?),
            CreateMultipartState::CheckSettings => self.settings_checked(result.value()?),
            CreateMultipartState::WriteUpload => {
                result.done()?;
                self.record_written()
            }
            CreateMultipartState::CountUpload => {
                result.done()?;
                self.upload_counted()
            }
            CreateMultipartState::CountInventory => self.inventory_counted(result),
            CreateMultipartState::WritePending => {
                result.done()?;
                self.created()
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    /// An opened provider upload goes, unless a commit was sent that may own it.
    fn abort(&mut self) -> Vec<IoCall> {
        let opened = self.backend_upload.take().filter(|_| !self.committing);
        let calls = opened.map(|backend_upload| {
            let abort = BlobEffect::AbortUpload {
                backend_upload: Box::new(backend_upload),
            };
            IoCall::from(Effect::Blob(abort))
        });
        calls.into_iter().collect()
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{CreateMultipartError, CreateMultipartInput, CreateMultipartOperation};
    use crate::groups::backends::BackendFenceError;
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::errors::BlobError;
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::keyspaces::{
        ABE_PENDING_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE,
    };
    use aruna_core::operation::EffectOperation;
    use aruna_core::structs::checksum::ChecksumAlgorithm;
    use aruna_core::structs::storage::abe::ObjectEnvelope;
    use aruna_core::structs::storage::backend_import::{PlainPath, tagged_path};
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::encryption::{
        BucketEncryption, BucketKeyRecord, EncryptionMode, SealPlan,
    };
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
    use aruna_core::structs::storage::multipart::{BackendUpload, MultipartUpload};
    use aruna_core::structs::storage::multipart::{MultipartChecksumHint, MultipartChecksumType};
    use aruna_core::structs::storage::routing::{
        BackendCatalog, GroupRoutingInputs, RoutingError, RoutingSnapshot, RoutingTarget,
        StorageRoutingRule,
    };
    use aruna_core::types::Effects;
    use aruna_core::types::TxnId;
    use std::collections::BTreeSet;
    use ulid::Ulid;

    fn input(snapshot: RoutingSnapshot) -> CreateMultipartInput {
        CreateMultipartInput {
            bucket: "bucket".to_string(),
            key: "archive/one".to_string(),
            group_id: snapshot.group_id,
            created_by: aruna_core::UserId::default(),
            checksum_hint: None,
            routing: snapshot,
        }
    }

    /// A bucket with no default refs, so the gate is skipped entirely.
    fn ungoverned_bucket() -> Event {
        bucket_read(None, None)
    }

    /// Answers the bucket and settings read.
    fn bucket_read(bucket: Option<Vec<u8>>, settings: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (b"bucket".to_vec().into(), bucket.map(Into::into)),
                (b"bucket".to_vec().into(), settings.map(Into::into)),
            ],
        })
    }

    /// Answers the purge fence read with no fence held.
    fn fence_clear() -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: crate::s3::purge_fence::fence_key("bucket"),
            value: None,
        })
    }

    fn snapshot() -> RoutingSnapshot {
        RoutingSnapshot::new(
            Ulid::from_parts(1, 1),
            BackendCatalog::new("default")
                .with_backend("default", None)
                .with_backend("tape", Some("archive".to_string())),
        )
    }

    fn rule(class: &str) -> StorageRoutingRule {
        StorageRoutingRule {
            key_prefix: String::new(),
            exact: false,
            target: RoutingTarget::Class(class.to_string()),
        }
    }

    #[test]
    fn pins_record_backend() {
        let snapshot = snapshot().with_bucket_rules(vec![rule("archive")]);
        let mut operation = CreateMultipartOperation::new(input(snapshot));
        operation.start();
        operation.step(ungoverned_bucket());
        operation.step(fence_clear());
        operation.step(opened(None));

        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));

        operation.step(fence_clear());
        let effects = operation.step(plain_settings());

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected one record write, got {effects:?}")
        };
        let record = MultipartUpload::from_bytes(value.as_ref()).unwrap();
        assert_eq!(record.backend, BackendRef::Node("tape".to_string()));
        assert_eq!(record.storage_class.as_deref(), Some("archive"));
    }

    #[test]
    fn mapped_refuses_layout() {
        use aruna_core::structs::storage::backend_import::{BackendImport, PLAIN_PATTERN};
        let backend_id = Ulid::from_parts(6, 6);
        let rule = BackendImport {
            import_id: Ulid::from_parts(5, 5),
            group_id: snapshot().group_id,
            backend_id,
            backend_prefix: "team-a-bucket/".to_string(),
            bucket: "bucket".to_string(),
            key_prefix: String::new(),
            schedule: None,
            created_by: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: PLAIN_PATTERN.to_string(),
        };
        // The upload resolves to the mapped storage itself, as a real mapped bucket does.
        let created = |key: &str| {
            let snapshot = snapshot().with_group_inputs(GroupRoutingInputs {
                default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id))),
                backend_ids: BTreeSet::from([backend_id]),
                imports: vec![rule.clone()],
                ..Default::default()
            });
            let mut input = input(snapshot);
            input.key = key.to_string();
            let mut operation = CreateMultipartOperation::new(input);
            operation.start();
            operation.step(ungoverned_bucket());
            let effects = operation.step(fence_clear());
            (operation, effects)
        };
        // A plain key checks its exact key before the provider upload opens there.
        let (mut operation, _) = created("data/a.csv");
        let effects = owner_answer(&mut operation, None);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::StatPlain { path, .. })] if path == "team-a-bucket/data/a.csv"
        ));
        // A folder key or a key without a plain path never opens an upload.
        for key in ["reports/", "reports//a.csv"] {
            let (operation, effects) = created(key);
            assert!(effects.is_empty(), "{key}: {effects:?}");
            assert!(matches!(
                operation.finalize(),
                Err(CreateMultipartError::Unmapped)
            ));
        }
    }

    #[test]
    fn passes_bucket_compression() {
        // The blob side opens no provider upload for a compressed bucket, so the parts are
        // composed into frames.
        let mut operation = CreateMultipartOperation::new(input(snapshot()));
        operation.start();
        let bucket = aruna_core::structs::storage::blob::BucketInfo {
            group_id: Ulid::from_parts(1, 1),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Zstd { level: 3 },
            distribution: None,
        };
        operation.step(bucket_read(Some(bucket.to_bytes().unwrap()), None));

        let effects = operation.step(fence_clear());

        let [Effect::Blob(BlobEffect::OpenUpload { resolved, .. })] = effects.as_slice() else {
            panic!("expected the upload to open, got {effects:?}")
        };
        assert_eq!(resolved.compression, Compression::Zstd { level: 3 });
    }

    /// Opens an upload of a distributed bucket up to its transactional authority read.
    fn distributed_until_check(bucket: &[u8], record: &[u8]) -> CreateMultipartOperation {
        use crate::distribution::test_record::authority_rows;
        let mut operation = CreateMultipartOperation::new(input(snapshot()));
        operation.start();
        operation.step(bucket_read(Some(bucket.to_vec()), None));
        operation.step(authority_rows(Some(record.to_vec()), None));
        operation.step(fence_clear());
        operation.step(opened(None));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        let effects = operation.step(fence_clear());
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::BatchRead { reads, txn_id: Some(_) })]
                    if reads.len() == 3
            ),
            "the transaction rereads bucket, record and fence: {effects:?}"
        );
        operation
    }

    #[test]
    fn upload_rechecks_fence() {
        use aruna_core::structs::storage::distribution::fence::{LocalFence, RangeFence};
        let (record, bytes) = crate::distribution::test_record::record(true);
        let binding = crate::distribution::test_record::binding(&record, 1, 5);
        let bucket = aruna_core::structs::storage::blob::BucketInfo {
            group_id: Ulid::from_parts(1, 1),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: Some(binding),
        };
        let bucket = bucket.to_bytes().unwrap();
        let rows = |fence: Option<Vec<u8>>| {
            Event::Storage(StorageEvent::BatchReadResult {
                values: vec![
                    (b"bucket".to_vec().into(), Some(bucket.clone().into())),
                    (b"record".to_vec().into(), Some(bytes.clone().into())),
                    (b"fence".to_vec().into(), fence.map(Into::into)),
                ],
            })
        };
        let mut open = distributed_until_check(&bucket, &bytes);
        let effects = open.step(rows(None));
        assert!(open.output.is_none(), "{effects:?}");
        // A transfer fence set after the first check refuses the upload in its transaction.
        let fence = LocalFence {
            ranges: vec![RangeFence {
                range_id: record.config.ranges[0].range_id,
                transfer_id: Ulid::from_parts(2, 2),
                epoch: record.config.ranges[0].epoch,
                since_ms: 0,
                uploads_open: true,
            }],
            ..LocalFence::default()
        };
        let mut fenced = distributed_until_check(&bucket, &bytes);
        fenced.step(rows(Some(fence.to_bytes().unwrap())));
        assert!(matches!(
            fenced.output,
            Some(Err(CreateMultipartError::Distribution(
                crate::distribution::guard::GuardError::Route(
                    aruna_core::structs::storage::distribution::RouteError::Moving(_)
                )
            )))
        ));
    }

    fn sealed_settings() -> (BucketEncryption, BucketKeyRecord) {
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(Ulid::from_bytes([7; 16])),
            key_generation: 2,
            storage_generation: 4,
            ..Default::default()
        };
        let key = settings.active_key().unwrap();
        (
            settings,
            BucketKeyRecord::new(key, Ulid::from_bytes([8; 16]), [5; 32], 0),
        )
    }

    /// Runs a sealed upload up to its record write and returns the effects of that step.
    fn sealed_create(hint: Option<MultipartChecksumHint>) -> (CreateMultipartOperation, Effects) {
        let (settings, record) = sealed_settings();
        let mut input = input(snapshot());
        input.checksum_hint = hint;
        let mut operation = CreateMultipartOperation::new(input);
        operation.start();
        let effects = operation.step(bucket_read(None, Some(settings.to_bytes().unwrap())));
        let [Effect::Storage(StorageEffect::Read { key_space, key, .. })] = effects.as_slice()
        else {
            panic!("expected the key record read, got {effects:?}")
        };
        assert_eq!(key_space, BUCKET_KEY_KEYSPACE);
        assert_eq!(key.as_ref(), settings.active_key().unwrap().key());
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: key.clone(),
            value: Some(record.to_bytes().unwrap().into()),
        }));
        (operation, effects)
    }

    /// Clears the open fence of a sealed upload and answers its ABE read and envelope effect.
    fn open_sealed(operation: &mut CreateMultipartOperation) -> Effects {
        use aruna_core::structs::storage::abe::{
            AbeEffect, AbeEvent, create_envelope, create_parameters,
        };
        let effects = operation.step(fence_clear());
        let [
            Effect::Storage(StorageEffect::BatchRead {
                reads,
                txn_id: None,
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the ABE read, got {effects:?}")
        };
        let key = sealed_settings().0.active_key().unwrap();
        let secret = aruna_core::compute::SecretBytes::new(vec![9; 32]);
        let realm = aruna_core::structs::identity::realm::RealmId::from_bytes([3; 32]);
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let parameters = create_parameters(&secret, realm, node, key).unwrap();
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    reads[0].1.clone(),
                    Some(parameters.to_bytes().unwrap().into()),
                ),
                (reads[1].1.clone(), Some(1u64.to_be_bytes().to_vec().into())),
            ],
        }));
        let [Effect::Blob(BlobEffect::Abe(effect))] = effects.as_slice() else {
            panic!("expected the envelope effect, got {effects:?}")
        };
        let AbeEffect::Envelope(plan) = effect.as_ref() else {
            panic!("expected an envelope plan, got {effect:?}")
        };
        assert_eq!(plan.bucket_public, [5; 32]);
        let (envelope, _) = create_envelope(plan.clone()).unwrap();
        let event = AbeEvent::Envelope(envelope);
        operation.step(Event::Blob(BlobEvent::Abe(Box::new(event))))
    }

    #[test]
    fn sealed_upload_snapshot() {
        // An encrypted upload opens no provider upload and records its plan after a reread.
        // Its pending envelope commits beside the record.
        let (settings, record) = sealed_settings();
        let (mut operation, _) = sealed_create(None);
        let effects = open_sealed(&mut operation);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
        let txn_id = TxnId::from_bytes([3u8; 16]);
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(fence_clear());
        let [
            Effect::Storage(StorageEffect::Read {
                key_space,
                txn_id: read_txn,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the settings reread, got {effects:?}")
        };
        assert_eq!(key_space, BUCKET_ENCRYPTION_KEYSPACE);
        assert_eq!(*read_txn, Some(txn_id));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(settings.to_bytes().unwrap().into()),
        }));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected the upload record, got {effects:?}")
        };
        let upload = MultipartUpload::from_bytes(value.as_ref()).unwrap();
        let plan = SealPlan::capture(&settings, &record).unwrap().unwrap();
        assert_eq!(
            upload.encryption.map(|encryption| encryption.plan),
            Some(plan)
        );
        assert_eq!(upload.backend_upload, None);
        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: b"upload".to_vec().into(),
        }));
        let effects = counted(&mut operation);
        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: write_txn,
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the pending envelope, got {effects:?}")
        };
        assert_eq!(key_space, ABE_PENDING_KEYSPACE);
        assert_eq!(key.as_ref(), upload.upload_id.to_bytes());
        assert_eq!(*write_txn, Some(txn_id));
        let envelope = ObjectEnvelope::from_bytes(value.as_ref()).unwrap();
        assert_eq!(envelope.context.object_key, upload.key);
        assert_eq!(envelope.context.epoch, 1);
        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: key.clone(),
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
        );
    }

    #[test]
    fn bucket_only_upload() {
        // A generation without admitted parameters commits its record without an envelope.
        let (settings, _) = sealed_settings();
        let (mut operation, _) = sealed_create(None);
        let effects = operation.step(fence_clear());
        let [Effect::Storage(StorageEffect::BatchRead { reads, .. })] = effects.as_slice() else {
            panic!("expected the ABE read, got {effects:?}")
        };
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: reads.iter().map(|(_, key)| (key.clone(), None)).collect(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
        let txn_id = TxnId::from_bytes([3u8; 16]);
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(fence_clear());
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(settings.to_bytes().unwrap().into()),
        }));
        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: b"upload".to_vec().into(),
        }));
        assert_eq!(operation.state, super::CreateMultipartState::CountUpload);
        let [Effect::Storage(StorageEffect::AddUsage { buckets, .. })] = effects.as_slice() else {
            panic!("expected the upload count, got {effects:?}")
        };
        let uploads: Vec<_> = buckets
            .iter()
            .map(|(_, delta)| delta.open_uploads)
            .collect();
        assert_eq!(uploads, [1]);
        assert_eq!(
            counted(&mut operation).as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
        );
    }

    /// Settles the open upload count the record write books.
    fn counted(operation: &mut CreateMultipartOperation) -> aruna_core::types::Effects {
        assert_eq!(operation.state, super::CreateMultipartState::CountUpload);
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }))
    }

    #[test]
    fn sealed_rotation_conflicts() {
        // A key generation that moved before the record commits fails the upload.
        let (mut settings, _) = sealed_settings();
        let (mut operation, _) = sealed_create(None);
        open_sealed(&mut operation);
        let txn_id = TxnId::from_bytes([3u8; 16]);
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(fence_clear());
        settings.key_generation = 3;
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(settings.to_bytes().unwrap().into()),
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        );
        assert!(matches!(
            operation.finalize(),
            Err(CreateMultipartError::BucketKey(_))
        ));
    }

    #[test]
    fn sealed_refuses_digest() {
        // Full-object SHA and MD5 cannot be checked without the plaintext; CRCs combine.
        let hint = |algorithm| MultipartChecksumHint {
            algorithm: Some(algorithm),
            checksum_type: MultipartChecksumType::FullObject,
        };
        let (operation, _) = sealed_create(Some(hint(ChecksumAlgorithm::Sha256)));
        assert_eq!(
            operation.finalize(),
            Err(CreateMultipartError::UnsupportedChecksum("SHA256"))
        );
        let (mut operation, _) = sealed_create(Some(hint(ChecksumAlgorithm::Crc64Nvme)));
        let effects = open_sealed(&mut operation);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
    }

    #[test]
    fn missing_class_pins() {
        // The pin records where the parts actually land, not what was asked.
        let snapshot = snapshot().with_bucket_rules(vec![rule("glacier")]);
        let mut operation = CreateMultipartOperation::new(input(snapshot));
        operation.start();
        operation.step(ungoverned_bucket());
        operation.step(fence_clear());
        operation.step(opened(None));

        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));

        operation.step(fence_clear());
        let effects = operation.step(plain_settings());

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected one record write, got {effects:?}")
        };
        let record = MultipartUpload::from_bytes(value.as_ref()).unwrap();
        assert_eq!(record.backend, BackendRef::Node("default".to_string()));
        assert_eq!(record.storage_class, None);
    }

    #[test]
    fn refuses_disabled_backend() {
        // The pinned backend must not outlive the tenant disabling it.
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let snapshot = snapshot().with_group_inputs(GroupRoutingInputs {
            default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id))),
            backend_ids: BTreeSet::from([backend_id]),
            ..Default::default()
        });
        let mut operation = CreateMultipartOperation::new(input(snapshot));
        operation.start();
        operation.step(ungoverned_bucket());
        operation.step(fence_clear());
        operation.step(opened(None));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(3),
        }));
        operation.step(fence_clear());

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(disabled(backend_id).to_bytes().unwrap().into()),
        }));

        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::AbortTransaction { .. })]
            ),
            "expected an abort, got {effects:?}"
        );
        assert!(matches!(
            operation.finalize(),
            Err(CreateMultipartError::BackendFenceError(
                BackendFenceError::Unavailable
            ))
        ));
    }

    /// The settings reread inside the record transaction, for a bucket without encryption.
    fn plain_settings() -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: None,
        })
    }

    #[test]
    fn plain_create_refused() {
        // Encryption enabled while the provider upload opened, before any upload row existed:
        // the plain upload never records and its provider upload is aborted.
        let mut operation = CreateMultipartOperation::new(input(snapshot()));
        operation.start();
        operation.step(ungoverned_bucket());
        operation.step(fence_clear());
        let upload = BackendUpload {
            location: aruna_core::structs::storage::blob::BackendLocation {
                backend: BackendRef::node_default(),
                storage_class: None,
                root: "/".to_string(),
                storage_bucket: "bucket".to_string(),
                backend_path: "bucket/key".to_string(),
                ulid: Ulid::from_bytes([6u8; 16]),
                format: aruna_core::structs::storage::format::StoredFormat::default(),
                created_by: aruna_core::UserId::default(),
                created_at: std::time::SystemTime::UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 0,
                hashes: std::collections::HashMap::new(),
            },
            upload_id: "provider".to_string(),
            record_id: Ulid::from_bytes([9u8; 16]),
        };
        operation.step(opened(Some(upload.clone())));
        let txn_id = TxnId::from_bytes([3u8; 16]);
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(fence_clear());
        let (settings, _) = sealed_settings();
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(settings.to_bytes().unwrap().into()),
        }));

        assert!(
            matches!(
                effects.as_slice(),
                [
                    Effect::Storage(StorageEffect::AbortTransaction { .. }),
                    Effect::Blob(BlobEffect::AbortUpload { backend_upload }),
                ] if **backend_upload == upload
            ),
            "expected the record and provider upload to be dropped, got {effects:?}"
        );
        assert!(matches!(
            operation.pending_error,
            Some(CreateMultipartError::BucketKey(_))
        ));
    }

    fn opened(backend_upload: Option<BackendUpload>) -> Event {
        Event::Blob(BlobEvent::UploadOpened { backend_upload })
    }

    #[test]
    fn refusal_aborts_provider() {
        // A record that never commits must not strand the provider upload it opened.
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let snapshot = snapshot().with_group_inputs(GroupRoutingInputs {
            default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id))),
            backend_ids: BTreeSet::from([backend_id]),
            ..Default::default()
        });
        let mut operation = CreateMultipartOperation::new(input(snapshot));
        operation.start();
        operation.step(ungoverned_bucket());
        let effects = operation.step(fence_clear());
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::OpenUpload { .. })]
        ));
        let upload = BackendUpload {
            location: aruna_core::structs::storage::blob::BackendLocation {
                backend: BackendRef::Group(backend_id),
                storage_class: None,
                root: "/".to_string(),
                storage_bucket: "tenant".to_string(),
                backend_path: "bucket/key".to_string(),
                ulid: Ulid::from_bytes([6u8; 16]),
                format: aruna_core::structs::storage::format::StoredFormat::default(),
                created_by: aruna_core::UserId::default(),
                created_at: std::time::SystemTime::UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 0,
                hashes: std::collections::HashMap::new(),
            },
            upload_id: "provider".to_string(),
            record_id: Ulid::from_bytes([9u8; 16]),
        };
        operation.step(opened(Some(upload.clone())));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(3),
        }));
        operation.step(fence_clear());

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(disabled(backend_id).to_bytes().unwrap().into()),
        }));
        assert!(
            matches!(
                effects.as_slice(),
                [
                    Effect::Storage(StorageEffect::AbortTransaction { .. }),
                    Effect::Blob(BlobEffect::AbortUpload { backend_upload }),
                ] if **backend_upload == upload
            ),
            "expected both aborts, got {effects:?}"
        );
        assert!(!operation.is_complete());
        operation.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: TxnId::from(3),
        }));
        operation.step(Event::Blob(BlobEvent::UploadAborted));

        assert!(matches!(
            operation.finalize(),
            Err(CreateMultipartError::BackendFenceError(
                BackendFenceError::Unavailable
            ))
        ));
    }

    /// An import rule writing `archive/` keys of `bucket` to `raw/` with `pattern`.
    fn plain_create(pattern: &str) -> (CreateMultipartOperation, Effects) {
        plain_create_at(pattern, None)
    }

    fn plain_create_at(pattern: &str, folder: Option<&str>) -> (CreateMultipartOperation, Effects) {
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let rule = aruna_core::structs::storage::backend_import::BackendImport {
            import_id: Ulid::from_bytes([4u8; 16]),
            group_id: Ulid::from_parts(1, 1),
            backend_id,
            backend_prefix: "raw/".to_string(),
            bucket: "bucket".to_string(),
            key_prefix: "archive/".to_string(),
            schedule: None,
            created_by: aruna_core::UserId::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: folder.map(str::to_string),
            write_pattern: pattern.to_string(),
        };
        let snapshot = snapshot().with_group_inputs(GroupRoutingInputs {
            default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id))),
            backend_ids: BTreeSet::from([backend_id]),
            imports: vec![rule],
            ..Default::default()
        });
        let mut operation = CreateMultipartOperation::new(input(snapshot));
        operation.start();
        operation.step(ungoverned_bucket());
        let effects = operation.step(fence_clear());
        (operation, effects)
    }

    /// Answers the read of the rule's record at the plain path with a row for `key`.
    fn owner_answer(operation: &mut CreateMultipartOperation, key: Option<&str>) -> Effects {
        let row = key.map(|key| {
            let id = Ulid::from_parts(8, 8);
            let row = aruna_core::structs::storage::backend_import::ImportedObject::written(
                id,
                key.to_string(),
                1,
                None,
            );
            row.to_bytes().unwrap().into()
        });
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: row,
        }))
    }

    fn opened_plain(effects: &Effects) -> PlainPath {
        match effects.as_slice() {
            [
                Effect::Blob(BlobEffect::OpenUpload {
                    plain: Some(plain), ..
                }),
            ] => plain.clone(),
            _ => panic!("expected a plain provider upload, got {effects:?}"),
        }
    }

    #[test]
    fn opens_plain_path() {
        // A same-name upload opens where a single write would go. A file the rule did not record
        // for this key keeps its path, with or without versions; the upload takes its tagged path.
        let own = Some("archive/one");
        let cases = [
            (own, false, None, false, true),
            (own, true, Some("v1".to_string()), false, false),
            (own, true, None, true, true),
            (Some("other/one"), true, Some("v1".to_string()), true, true),
            (None, true, Some("v1".to_string()), true, true),
        ];
        for (owner, exists, version, tagged, if_not_exists) in cases {
            let (mut operation, effects) = plain_create("{dir}{name}");
            assert!(
                matches!(
                    effects.as_slice(),
                    [Effect::Storage(StorageEffect::Read { .. })]
                ),
                "expected the owner read, got {effects:?}"
            );
            let effects = owner_answer(&mut operation, owner);
            assert!(
                matches!(
                    effects.as_slice(),
                    [Effect::Blob(BlobEffect::StatPlain { path, .. })] if path == "raw/one"
                ),
                "expected the path stat, got {effects:?}"
            );
            let effects = operation.step(Event::Blob(BlobEvent::PlainStat { exists, version }));
            let plain = opened_plain(&effects);
            let expected = match tagged {
                true => tagged_path("raw/one", plain.ulid),
                false => "raw/one".to_string(),
            };
            assert_eq!(plain.path, expected);
            assert_eq!(plain.if_not_exists, if_not_exists);
        }
    }

    #[test]
    fn tags_unrecorded_path() {
        // A separate write folder has no record to prove the file there the upload's own.
        let (mut operation, effects) = plain_create_at("{dir}{name}", Some("out/"));
        assert!(
            matches!(
                effects.as_slice(),
                [Effect::Blob(BlobEffect::StatPlain { path, .. })] if path == "out/one"
            ),
            "expected the path stat, got {effects:?}"
        );
        let effects = operation.step(Event::Blob(BlobEvent::PlainStat {
            exists: true,
            version: Some("v1".to_string()),
        }));
        let plain = opened_plain(&effects);
        assert_eq!(plain.path, tagged_path("out/one", plain.ulid));
    }

    #[test]
    fn opens_versioned_path() {
        // An `{id}` pattern names a path of its own, fixed by the version id picked now.
        let (_, effects) = plain_create("{id}_{name}");
        let plain = opened_plain(&effects);
        assert_eq!(plain.path, format!("raw/{}_one", plain.ulid));
    }

    #[test]
    fn plain_stat_fails() {
        let (mut operation, _) = plain_create("{dir}{name}");
        owner_answer(&mut operation, None);
        let effects = operation.step(Event::Blob(BlobEvent::Error(BlobError::KeyTooLong)));
        assert!(effects.is_empty(), "nothing was opened yet: {effects:?}");
        assert!(matches!(
            operation.finalize(),
            Err(CreateMultipartError::BlobError(BlobError::KeyTooLong))
        ));
    }

    fn disabled(backend_id: Ulid) -> GroupStorage {
        GroupStorage {
            backend_id,
            group_id: Ulid::from_bytes([7u8; 16]),
            name: "tenant".to_string(),
            kind: GroupBackendKind::S3,
            public_config: std::collections::HashMap::new(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            disabled: true,
            cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
        }
    }

    #[test]
    fn unknown_backend_aborts() {
        // A refused backend must fail before a transaction exists to leak.
        let snapshot = snapshot().with_bucket_rules(vec![StorageRoutingRule {
            key_prefix: String::new(),
            exact: false,
            target: RoutingTarget::Backend(BackendRef::Node("ghost".to_string())),
        }]);
        let mut operation = CreateMultipartOperation::new(input(snapshot));

        let effects = operation.start();

        assert!(effects.is_empty(), "expected no effects, got {effects:?}");
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(CreateMultipartError::RoutingFailed(
                RoutingError::UnknownBackend(_)
            ))
        ));
    }
}
