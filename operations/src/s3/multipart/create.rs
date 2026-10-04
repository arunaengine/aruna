//! Starts a multipart upload, gating placement and pinning the backend on the upload record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::groups::backends::{BackendFenceError, check_fence, fence_backend};
use crate::placement::policy::{
    GateContext, GatedBucket, PolicyGateError, PolicyGateOperation, gate_decision, write_gate,
};
use crate::s3::bucket::key_rows::settings_read;
use crate::s3::purge_fence::{PurgeFenceError, check_write_fence, write_fence_read};
use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE, UPLOAD_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{BucketInfo, ResolvedBackend};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyError, BucketKeyRecord, SealPlan,
};
use aruna_core::structs::storage::multipart::{
    BackendUpload, MultipartChecksumHint, MultipartChecksumType, MultipartUpload,
    MultipartUploadStatus, UploadEncryption,
};
use aruna_core::structs::storage::routing::{RoutingError, RoutingSnapshot, resolve_backend};
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;
use std::collections::HashMap;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateMultipartState {
    Init,
    ReadGateBucket,
    ReadBucketKey,
    PolicyGate,
    CheckOpenFence,
    OpenUpload,
    StartTransaction,
    CheckPurgeFence,
    FenceBackend,
    CheckSettings,
    WriteUpload,
    CommitTransaction,
    AbortBackendUpload,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateMultipartError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Transaction id missing")]
    TransactionMissing,
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: CreateMultipartState,
        expected: &'static str,
        received: Event,
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
    #[error("operation did not finish")]
    NotFinished,
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
    txn_id: Option<TxnId>,
    resolved: Option<ResolvedBackend>,
    record: Option<MultipartUpload>,
    metadata: HashMap<String, String>,
    /// Destination details of this node. Absent fails every governed upload
    /// closed and leaves the ungoverned path untouched.
    gate_context: Option<GateContext>,
    gate: Option<PolicyGateOperation>,
    /// Refs and subject the gate admitted, stored on the upload record so every
    /// part and the completion inherit exactly what was evaluated here.
    stored_policies: Vec<PlacementPolicyRef>,
    stored_subject: u64,
    /// The provider upload opened for this record; aborted unless a commit may own it.
    backend_upload: Option<BackendUpload>,
    /// Bucket and settings read before the gate, kept until the key record completes the plan.
    settings: Option<(Option<BucketInfo>, BucketEncryption)>,
    encryption: Option<UploadEncryption>,
    pending_error: Option<CreateMultipartError>,
    output: Option<Result<CreateMultipartResult, CreateMultipartError>>,
}

impl CreateMultipartOperation {
    pub fn new(input: CreateMultipartInput) -> Self {
        Self {
            input,
            upload_id: Ulid::generate(),
            state: CreateMultipartState::Init,
            txn_id: None,
            resolved: None,
            record: None,
            metadata: HashMap::new(),
            gate_context: None,
            gate: None,
            stored_policies: Vec::new(),
            stored_subject: 0,
            backend_upload: None,
            settings: None,
            encryption: None,
            pending_error: None,
            output: None,
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

    fn emit_error(&mut self, error: CreateMultipartError) -> Effects {
        if let Some(backend_upload) = self.backend_upload.take() {
            self.pending_error = Some(error);
            self.state = CreateMultipartState::AbortBackendUpload;
            let mut effects = self.abort();
            effects.push(Effect::Blob(BlobEffect::AbortUpload {
                backend_upload: Box::new(backend_upload),
            }));
            return effects;
        }
        self.state = CreateMultipartState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    /// Waits out the transaction abort, then reports the error that started the cleanup.
    fn backend_aborted(&mut self, event: Event) -> Effects {
        if matches!(
            event,
            Event::Storage(StorageEvent::TransactionAborted { .. } | StorageEvent::Error { .. })
        ) {
            return smallvec![];
        }
        let error = self
            .pending_error
            .take()
            .unwrap_or(CreateMultipartError::CreateUploadFailed);
        self.emit_error(error)
    }

    /// A fenced destination opens no provider upload; the transaction checks the fence again.
    fn check_open_fence(&mut self) -> Effects {
        self.state = CreateMultipartState::CheckOpenFence;
        smallvec![write_fence_read(&self.input.bucket, None)]
    }

    /// S3 backends open the provider upload the parts stream into; others answer `None`.
    fn open_upload(&mut self, event: Event) -> Effects {
        if let Err(error) = check_write_fence(event, &self.input.bucket, &self.input.key) {
            return self.emit_error(error.into());
        }
        // Encrypted parts are sealed pieces of their own; completion composes them.
        if self.encryption.is_some() {
            return self.start_transaction();
        }
        let Some(resolved) = self.resolved.clone() else {
            return self.emit_error(CreateMultipartError::CreateUploadFailed);
        };
        self.state = CreateMultipartState::OpenUpload;
        smallvec![Effect::Blob(BlobEffect::OpenUpload {
            record_id: self.upload_id,
            bucket: self.input.bucket.clone(),
            key: self.input.key.clone(),
            resolved,
            created_by: self.input.created_by,
        })]
    }

    fn upload_opened(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::UploadOpened { backend_upload }) => {
                self.backend_upload = backend_upload;
                self.start_transaction()
            }
            Event::Blob(BlobEvent::Error(error)) => self.emit_error(error.into()),
            event => self.emit_error(CreateMultipartError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Blob(BlobEvent::UploadOpened)",
                received: event,
            }),
        }
    }

    fn handle_init(&mut self) -> Effects {
        // Routing is fallible, so it resolves before a transaction exists to
        // abort.
        let resolved =
            match resolve_backend(&self.input.routing, &self.input.bucket, &self.input.key) {
                Ok(resolved) => resolved,
                Err(error) => return self.emit_error(error.into()),
            };
        self.resolved = Some(resolved);
        // The destination default is read before the upload exists, so no part
        // can ever be written under a rule this node was never admitted for.
        self.state = CreateMultipartState::ReadGateBucket;
        smallvec![settings_read(&self.input.bucket, None)]
    }

    /// Reads the bucket and its encryption settings; an encrypted bucket also needs its key.
    fn handle_gate_bucket(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.emit_error(CreateMultipartError::CreateUploadFailed);
        };
        let [(_, bucket), (_, settings)] = match <[_; 2]>::try_from(values) {
            Ok(values) => values,
            Err(_) => return self.emit_error(CreateMultipartError::CreateUploadFailed),
        };
        let parsed = bucket
            .as_ref()
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()
            .and_then(|bucket| Ok((bucket, BucketEncryption::from_row(settings.as_deref())?)));
        let (bucket, settings) = match parsed {
            Ok(parsed) => parsed,
            Err(error) => return self.emit_error(error.into()),
        };
        let Some(key) = settings.active_key() else {
            return self.gate_bucket(bucket);
        };
        self.settings = Some((bucket, settings));
        self.state = CreateMultipartState::ReadBucketKey;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BUCKET_KEY_KEYSPACE.to_string(),
            key: key.key().into(),
            txn_id: None,
        })]
    }

    /// Captures the seal plan from the active key record, then gates the bucket.
    fn handle_bucket_key(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(CreateMultipartError::CreateUploadFailed);
        };
        let Some((bucket, settings)) = self.settings.take() else {
            return self.emit_error(CreateMultipartError::CreateUploadFailed);
        };
        let Some(value) = value else {
            return self.emit_error(BucketKeyError::Unsupported.into());
        };
        let plan = BucketKeyRecord::from_bytes(value.as_ref())
            .map_err(CreateMultipartError::from)
            .and_then(|record| Ok(SealPlan::capture(&settings, &record)?));
        if let Some(algorithm) = self.input.checksum_hint.as_ref().and_then(full_digest) {
            return self.emit_error(CreateMultipartError::UnsupportedChecksum(
                algorithm.s3_name(),
            ));
        }
        match plan {
            Ok(Some(plan)) => {
                let compression = bucket.as_ref().map(|bucket| bucket.compression);
                self.encryption = Some(UploadEncryption {
                    plan,
                    compression: compression.unwrap_or_default(),
                });
                self.gate_bucket(bucket)
            }
            Ok(None) => self.emit_error(BucketKeyError::Unsupported.into()),
            Err(error) => self.emit_error(error),
        }
    }

    fn gate_bucket(&mut self, bucket: Option<BucketInfo>) -> Effects {
        self.stored_policies = GatedBucket::observe(bucket.as_ref()).policies;
        let compression = bucket.as_ref().map(|bucket| bucket.compression);
        self.resolved = self
            .resolved
            .take()
            .map(|resolved| resolved.with_compression(compression.unwrap_or_default()));
        let group_id = bucket
            .as_ref()
            .map_or(self.input.group_id, |bucket| bucket.group_id);
        match write_gate(
            self.gate_context.as_ref(),
            &self.stored_policies,
            Some(group_id),
        ) {
            Ok(None) => self.check_open_fence(),
            Ok(Some(mut gate)) => {
                let effects = gate.start();
                let complete = gate.is_complete();
                self.gate = Some(gate);
                self.state = CreateMultipartState::PolicyGate;
                match complete {
                    true => self.finish_gate(),
                    false => effects,
                }
            }
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn handle_policy_gate(&mut self, event: Event) -> Effects {
        let Some(gate) = self.gate.as_mut() else {
            return self.emit_error(CreateMultipartError::CreateUploadFailed);
        };
        let effects = gate.step(event);
        match gate.is_complete() {
            true => self.finish_gate(),
            false => effects,
        }
    }

    fn finish_gate(&mut self) -> Effects {
        let Some(gate) = self.gate.take() else {
            return self.emit_error(CreateMultipartError::CreateUploadFailed);
        };
        let outcome = match gate.finalize() {
            Ok(outcome) => outcome,
            Err(error) => return self.emit_error(PolicyGateError::from(error).into()),
        };
        match gate_decision(outcome) {
            Ok(()) => {
                self.stored_subject = self
                    .gate_context
                    .as_ref()
                    .map_or(0, |context| context.subject.generation);
                self.check_open_fence()
            }
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn start_transaction(&mut self) -> Effects {
        self.state = CreateMultipartState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(CreateMultipartError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };

        self.txn_id = Some(txn_id);
        self.state = CreateMultipartState::CheckPurgeFence;
        smallvec![write_fence_read(&self.input.bucket, self.txn_id)]
    }

    fn fence_checked(&mut self, event: Event) -> Effects {
        if let Err(error) = check_write_fence(event, &self.input.bucket, &self.input.key) {
            return self.emit_error(error.into());
        }
        let Some(resolved) = self.resolved.as_ref() else {
            return self.emit_error(CreateMultipartError::CreateUploadFailed);
        };
        match fence_backend(&resolved.backend, self.txn_id) {
            Some(effect) => {
                self.state = CreateMultipartState::FenceBackend;
                smallvec![effect]
            }
            None => self.check_settings(),
        }
    }

    fn handle_backend_fenced(&mut self, event: Event) -> Effects {
        match check_fence(event) {
            Ok(()) => self.check_settings(),
            Err(error) => self.emit_error(error.into()),
        }
    }

    /// Rereads the settings in the transaction, so a mode change or rotation that started
    /// meanwhile conflicts instead of leaving an upload with a stale plan.
    fn check_settings(&mut self) -> Effects {
        if self.encryption.is_none() {
            return self.write_upload();
        }
        self.state = CreateMultipartState::CheckSettings;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            key: self.input.bucket.as_bytes().into(),
            txn_id: self.txn_id,
        })]
    }

    fn settings_checked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(CreateMultipartError::CreateUploadFailed);
        };
        let Some(encryption) = self.encryption else {
            return self.emit_error(CreateMultipartError::CreateUploadFailed);
        };
        let current = BucketEncryption::from_row(value.as_deref())
            .map_err(CreateMultipartError::from)
            .and_then(|settings| Ok(encryption.plan.still_current(&settings)?));
        match current {
            Ok(()) => self.write_upload(),
            Err(error) => self.emit_error(error),
        }
    }

    fn write_upload(&mut self) -> Effects {
        let Some((txn_id, resolved)) = self.txn_id.zip(self.resolved.take()) else {
            return self.emit_error(CreateMultipartError::CreateUploadFailed);
        };
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
        let value = match record.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.emit_error(err.into()),
        };

        self.record = Some(record.clone());
        self.state = CreateMultipartState::WriteUpload;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: UPLOAD_KEYSPACE.to_string(),
            key: record.upload_id.to_bytes().to_vec().into(),
            value: value.into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_record_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(CreateMultipartError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::WriteResult)",
                received: event,
            });
        };

        let Some(txn_id) = self.txn_id else {
            return self.emit_error(CreateMultipartError::TransactionMissing);
        };
        self.state = CreateMultipartState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(CreateMultipartError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };

        // The committed record owns the provider upload from here on.
        self.backend_upload = None;
        let Some(record) = self.record.clone() else {
            return self.emit_error(CreateMultipartError::CreateUploadFailed);
        };
        self.txn_id = None;
        self.state = CreateMultipartState::Finish;
        self.output = Some(Ok(CreateMultipartResult { record }));
        smallvec![]
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

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.state == CreateMultipartState::AbortBackendUpload {
            return self.backend_aborted(event);
        }
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            // A commit that may have landed leaves the provider upload to its record.
            if self.state == CreateMultipartState::CommitTransaction && !error.proves_no_commit() {
                self.backend_upload = None;
            }
            return self.emit_error(error.clone().into());
        }
        match self.state {
            CreateMultipartState::Init => self.handle_init(),
            CreateMultipartState::ReadGateBucket => self.handle_gate_bucket(event),
            CreateMultipartState::ReadBucketKey => self.handle_bucket_key(event),
            CreateMultipartState::PolicyGate => self.handle_policy_gate(event),
            CreateMultipartState::CheckOpenFence => self.open_upload(event),
            CreateMultipartState::OpenUpload => self.upload_opened(event),
            CreateMultipartState::StartTransaction => self.handle_transaction_started(event),
            CreateMultipartState::CheckPurgeFence => self.fence_checked(event),
            CreateMultipartState::FenceBackend => self.handle_backend_fenced(event),
            CreateMultipartState::CheckSettings => self.settings_checked(event),
            CreateMultipartState::WriteUpload => self.handle_record_written(event),
            CreateMultipartState::CommitTransaction => self.handle_transaction_committed(event),
            CreateMultipartState::AbortBackendUpload => self.backend_aborted(event),
            CreateMultipartState::Finish => smallvec![],
            CreateMultipartState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateMultipartState::Finish | CreateMultipartState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Err(CreateMultipartError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        let mut effects: Effects = self
            .txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            });
        if let Some(backend_upload) = self.backend_upload.take() {
            effects.push(Effect::Blob(BlobEffect::AbortUpload {
                backend_upload: Box::new(backend_upload),
            }));
        }
        effects
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{CreateMultipartError, CreateMultipartInput, CreateMultipartOperation};
    use crate::groups::backends::BackendFenceError;
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::keyspaces::{BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE};
    use aruna_core::operation::Operation;
    use aruna_core::structs::checksum::ChecksumAlgorithm;
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

        let effects = operation.step(fence_clear());

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected one record write, got {effects:?}")
        };
        let record = MultipartUpload::from_bytes(value.as_ref()).unwrap();
        assert_eq!(record.backend, BackendRef::Node("tape".to_string()));
        assert_eq!(record.storage_class.as_deref(), Some("archive"));
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
        };
        operation.step(bucket_read(Some(bucket.to_bytes().unwrap()), None));

        let effects = operation.step(fence_clear());

        let [Effect::Blob(BlobEffect::OpenUpload { resolved, .. })] = effects.as_slice() else {
            panic!("expected the upload to open, got {effects:?}")
        };
        assert_eq!(resolved.compression, Compression::Zstd { level: 3 });
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

    #[test]
    fn sealed_upload_snapshot() {
        // An encrypted upload opens no provider upload and records its plan after a reread.
        let (settings, record) = sealed_settings();
        let (mut operation, _) = sealed_create(None);
        let effects = operation.step(fence_clear());
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
    }

    #[test]
    fn sealed_rotation_conflicts() {
        // A key generation that moved before the record commits fails the upload.
        let (mut settings, _) = sealed_settings();
        let (mut operation, _) = sealed_create(None);
        operation.step(fence_clear());
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
        let effects = operation.step(fence_clear());
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

        let effects = operation.step(fence_clear());

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
