use crate::group_backends::{BackendFenceError, check_fence, fence_backend};
use crate::placement_policy::{
    GateContext, GatedBucket, PolicyGateError, PolicyGateOperation, gate_decision, write_gate,
};
use crate::s3::purge_fence::{PurgeFenceError, check_write_fence, write_fence_read};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{S3_BUCKET_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    BucketInfo, MultipartUpload, MultipartUploadChecksumHint, MultipartUploadStatus,
    PlacementPolicyRef, ResolvedBackend, RoutingError, RoutingSnapshot, resolve_backend,
};
use aruna_core::types::{Effects, GroupId, TxnId, UserId};
use smallvec::smallvec;
use std::collections::HashMap;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateMultipartUploadState {
    Init,
    ReadGateBucket,
    PolicyGate,
    StartTransaction,
    CheckPurgeFence,
    FenceBackend,
    WriteUpload,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateMultipartUploadError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Transaction id missing")]
    TransactionMissing,
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: CreateMultipartUploadState,
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
    CreateMultipartUploadFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateMultipartUploadInput {
    pub bucket: String,
    pub key: String,
    pub group_id: GroupId,
    pub created_by: UserId,
    pub checksum_hint: Option<MultipartUploadChecksumHint>,
    /// Routing inputs; the resolved backend is pinned on the upload record so
    /// every part and the composed object follow it.
    pub routing: RoutingSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateMultipartUploadResult {
    pub record: MultipartUpload,
}

#[derive(Debug, PartialEq)]
pub struct CreateMultipartUploadOperation {
    input: CreateMultipartUploadInput,
    state: CreateMultipartUploadState,
    txn_id: Option<TxnId>,
    resolved: Option<ResolvedBackend>,
    record: Option<MultipartUpload>,
    metadata: HashMap<String, String>,
    /// Destination facts of this node. Absent fails every governed upload
    /// closed and leaves the ungoverned path untouched.
    gate_context: Option<GateContext>,
    gate: Option<PolicyGateOperation>,
    /// Refs and subject the gate admitted, sealed on the upload record so every
    /// part and the completion inherit exactly what was evaluated here.
    sealed_policies: Vec<PlacementPolicyRef>,
    sealed_subject: u64,
    output: Option<Result<CreateMultipartUploadResult, CreateMultipartUploadError>>,
}

impl CreateMultipartUploadOperation {
    pub fn new(input: CreateMultipartUploadInput) -> Self {
        Self {
            input,
            state: CreateMultipartUploadState::Init,
            txn_id: None,
            resolved: None,
            record: None,
            metadata: HashMap::new(),
            gate_context: None,
            gate: None,
            sealed_policies: Vec::new(),
            sealed_subject: 0,
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

    fn emit_error(&mut self, error: CreateMultipartUploadError) -> Effects {
        self.state = CreateMultipartUploadState::Error;
        self.output = Some(Err(error));
        self.abort()
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
        self.state = CreateMultipartUploadState::ReadGateBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.input.bucket.as_bytes().into(),
            txn_id: None,
        })]
    }

    fn handle_gate_bucket(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(CreateMultipartUploadError::CreateMultipartUploadFailed);
        };
        let bucket = match value
            .as_ref()
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(bucket) => bucket,
            Err(error) => return self.emit_error(error.into()),
        };
        self.sealed_policies = GatedBucket::observe(bucket.as_ref()).policies;
        match write_gate(self.gate_context.as_ref(), &self.sealed_policies) {
            Ok(None) => self.start_transaction(),
            Ok(Some(mut gate)) => {
                let effects = gate.start();
                let complete = gate.is_complete();
                self.gate = Some(gate);
                self.state = CreateMultipartUploadState::PolicyGate;
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
            return self.emit_error(CreateMultipartUploadError::CreateMultipartUploadFailed);
        };
        let effects = gate.step(event);
        match gate.is_complete() {
            true => self.finish_gate(),
            false => effects,
        }
    }

    fn finish_gate(&mut self) -> Effects {
        let Some(gate) = self.gate.take() else {
            return self.emit_error(CreateMultipartUploadError::CreateMultipartUploadFailed);
        };
        let outcome = match gate.finalize() {
            Ok(outcome) => outcome,
            Err(error) => return self.emit_error(PolicyGateError::from(error).into()),
        };
        match gate_decision(outcome.decision) {
            Ok(()) => {
                self.sealed_subject = self
                    .gate_context
                    .as_ref()
                    .map_or(0, |context| context.subject.generation);
                self.start_transaction()
            }
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn start_transaction(&mut self) -> Effects {
        self.state = CreateMultipartUploadState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(CreateMultipartUploadError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };

        self.txn_id = Some(txn_id);
        self.state = CreateMultipartUploadState::CheckPurgeFence;
        smallvec![write_fence_read(&self.input.bucket, self.txn_id)]
    }

    fn handle_purge_fence_checked(&mut self, event: Event) -> Effects {
        if let Err(error) = check_write_fence(event, &self.input.bucket, &self.input.key) {
            return self.emit_error(error.into());
        }
        let Some(resolved) = self.resolved.as_ref() else {
            return self.emit_error(CreateMultipartUploadError::CreateMultipartUploadFailed);
        };
        match fence_backend(&resolved.backend, self.txn_id) {
            Some(effect) => {
                self.state = CreateMultipartUploadState::FenceBackend;
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
            return self.emit_error(CreateMultipartUploadError::CreateMultipartUploadFailed);
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
            placement_policies: self.sealed_policies.clone(),
            subject_generation: self.sealed_subject,
        };
        let value = match record.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.emit_error(err.into()),
        };

        self.record = Some(record.clone());
        self.state = CreateMultipartUploadState::WriteUpload;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            key: record.upload_id.to_bytes().to_vec().into(),
            value: value.into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_record_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(CreateMultipartUploadError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::WriteResult)",
                received: event,
            });
        };

        let Some(txn_id) = self.txn_id else {
            return self.emit_error(CreateMultipartUploadError::TransactionMissing);
        };
        self.state = CreateMultipartUploadState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(CreateMultipartUploadError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };

        let Some(record) = self.record.clone() else {
            return self.emit_error(CreateMultipartUploadError::CreateMultipartUploadFailed);
        };
        self.txn_id = None;
        self.state = CreateMultipartUploadState::Finish;
        self.output = Some(Ok(CreateMultipartUploadResult { record }));
        smallvec![]
    }
}

impl Operation for CreateMultipartUploadOperation {
    type Output = Option<Result<CreateMultipartUploadResult, CreateMultipartUploadError>>;
    type Error = CreateMultipartUploadError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            CreateMultipartUploadState::Init => self.handle_init(),
            CreateMultipartUploadState::ReadGateBucket => self.handle_gate_bucket(event),
            CreateMultipartUploadState::PolicyGate => self.handle_policy_gate(event),
            CreateMultipartUploadState::StartTransaction => self.handle_transaction_started(event),
            CreateMultipartUploadState::CheckPurgeFence => self.handle_purge_fence_checked(event),
            CreateMultipartUploadState::FenceBackend => self.handle_backend_fenced(event),
            CreateMultipartUploadState::WriteUpload => self.handle_record_written(event),
            CreateMultipartUploadState::CommitTransaction => {
                self.handle_transaction_committed(event)
            }
            CreateMultipartUploadState::Finish => smallvec![],
            CreateMultipartUploadState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateMultipartUploadState::Finish | CreateMultipartUploadState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == CreateMultipartUploadState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(CreateMultipartUploadError::CreateMultipartUploadFailed);
        }

        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CreateMultipartUploadError, CreateMultipartUploadInput, CreateMultipartUploadOperation,
    };
    use crate::group_backends::BackendFenceError;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        BackendCatalog, BackendRef, GroupBackendKind, GroupRoutingInputs, GroupStorageBackend,
        MultipartUpload, RoutingError, RoutingSnapshot, RoutingTarget, StorageRoutingRule,
    };
    use aruna_core::types::TxnId;
    use std::collections::BTreeSet;
    use ulid::Ulid;

    fn input(snapshot: RoutingSnapshot) -> CreateMultipartUploadInput {
        CreateMultipartUploadInput {
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
            Ulid::generate(),
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
        let mut operation = CreateMultipartUploadOperation::new(input(snapshot));
        operation.start();
        operation.step(ungoverned_bucket());

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
        let mut operation = CreateMultipartUploadOperation::new(input(snapshot));
        operation.start();
        operation.step(ungoverned_bucket());

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
        let mut operation = CreateMultipartUploadOperation::new(input(snapshot));
        operation.start();
        operation.step(ungoverned_bucket());
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
            Err(CreateMultipartUploadError::BackendFenceError(
                BackendFenceError::Unavailable
            ))
        ));
    }

    fn disabled(backend_id: Ulid) -> GroupStorageBackend {
        GroupStorageBackend {
            backend_id,
            group_id: Ulid::from_bytes([7u8; 16]),
            name: "tenant".to_string(),
            kind: GroupBackendKind::S3,
            public_config: std::collections::HashMap::new(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            disabled: true,
            cleanup: aruna_core::structs::CleanupStrategy::Retain,
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
        let mut operation = CreateMultipartUploadOperation::new(input(snapshot));

        let effects = operation.start();

        assert!(effects.is_empty(), "expected no effects, got {effects:?}");
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(CreateMultipartUploadError::RoutingFailed(
                RoutingError::UnknownBackend(_)
            ))
        ));
    }
}
