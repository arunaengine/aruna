//! Starts a multipart upload, gating placement and pinning the backend on the upload record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::groups::backends::{BackendFenceError, check_fence, fence_backend};
use crate::placement::policy::{
    GateContext, GatedBucket, PolicyGateError, PolicyGateOperation, gate_decision, write_gate,
};
use crate::s3::purge_fence::{PurgeFenceError, check_write_fence, write_fence_read};
use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{S3_BUCKET_KEYSPACE, UPLOAD_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{BucketInfo, ResolvedBackend};
use aruna_core::structs::storage::multipart::{
    BackendUpload, MultipartChecksumHint, MultipartUpload, MultipartUploadStatus,
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
    PolicyGate,
    CheckOpenFence,
    OpenUpload,
    StartTransaction,
    CheckPurgeFence,
    FenceBackend,
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
    pending_error: Option<CreateMultipartError>,
    output: Option<Result<CreateMultipartResult, CreateMultipartError>>,
}

impl CreateMultipartOperation {
    pub fn new(input: CreateMultipartInput) -> Self {
        Self {
            input,
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
            effects.push(Effect::Blob(BlobEffect::AbortUpload { backend_upload }));
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
        let Some(resolved) = self.resolved.clone() else {
            return self.emit_error(CreateMultipartError::CreateUploadFailed);
        };
        self.state = CreateMultipartState::OpenUpload;
        smallvec![Effect::Blob(BlobEffect::OpenUpload {
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
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.input.bucket.as_bytes().into(),
            txn_id: None,
        })]
    }

    fn handle_gate_bucket(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(CreateMultipartError::CreateUploadFailed);
        };
        let bucket = match value
            .as_ref()
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(bucket) => bucket,
            Err(error) => return self.emit_error(error.into()),
        };
        self.stored_policies = GatedBucket::observe(bucket.as_ref()).policies;
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
            None => self.write_upload(),
        }
    }

    fn handle_backend_fenced(&mut self, event: Event) -> Effects {
        match check_fence(event) {
            Ok(()) => self.write_upload(),
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn write_upload(&mut self) -> Effects {
        let Some((txn_id, resolved)) = self.txn_id.zip(self.resolved.take()) else {
            return self.emit_error(CreateMultipartError::CreateUploadFailed);
        };
        let record = MultipartUpload {
            backend: resolved.backend,
            storage_class: resolved.storage_class,
            upload_id: Ulid::generate(),
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
            CreateMultipartState::PolicyGate => self.handle_policy_gate(event),
            CreateMultipartState::CheckOpenFence => self.open_upload(event),
            CreateMultipartState::OpenUpload => self.upload_opened(event),
            CreateMultipartState::StartTransaction => self.handle_transaction_started(event),
            CreateMultipartState::CheckPurgeFence => self.fence_checked(event),
            CreateMultipartState::FenceBackend => self.handle_backend_fenced(event),
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
            effects.push(Effect::Blob(BlobEffect::AbortUpload { backend_upload }));
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
    use aruna_core::operation::Operation;
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
    use aruna_core::structs::storage::multipart::{BackendUpload, MultipartUpload};
    use aruna_core::structs::storage::routing::{
        BackendCatalog, GroupRoutingInputs, RoutingError, RoutingSnapshot, RoutingTarget,
        StorageRoutingRule,
    };
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
        Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: None,
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
                compressed: false,
                encrypted: false,
                created_by: aruna_core::UserId::default(),
                created_at: std::time::SystemTime::UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 0,
                hashes: std::collections::HashMap::new(),
            },
            upload_id: "provider".to_string(),
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
                ] if *backend_upload == upload
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
