use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_MULTIPART_UPLOAD_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{
    MultipartUpload, MultipartUploadChecksumHint, MultipartUploadStatus, RoutingError,
    RoutingSnapshot, resolve_backend,
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
    StartTransaction,
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
    record: Option<MultipartUpload>,
    metadata: HashMap<String, String>,
    output: Option<Result<CreateMultipartUploadResult, CreateMultipartUploadError>>,
}

impl CreateMultipartUploadOperation {
    pub fn new(input: CreateMultipartUploadInput) -> Self {
        Self {
            input,
            state: CreateMultipartUploadState::Init,
            txn_id: None,
            record: None,
            metadata: HashMap::new(),
            output: None,
        }
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    fn emit_error(&mut self, error: CreateMultipartUploadError) -> Effects {
        self.state = CreateMultipartUploadState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn handle_init(&mut self) -> Effects {
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

        let resolved =
            match resolve_backend(&self.input.routing, &self.input.bucket, &self.input.key) {
                Ok(resolved) => resolved,
                Err(error) => return self.emit_error(error.into()),
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
        };
        let value = match record.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.emit_error(err.into()),
        };

        self.txn_id = Some(txn_id);
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
            CreateMultipartUploadState::StartTransaction => self.handle_transaction_started(event),
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
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        BackendCatalog, BackendRef, MultipartUpload, RoutingError, RoutingSnapshot, RoutingTarget,
        StorageRoutingRule,
    };
    use aruna_core::types::TxnId;
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
    fn pins_backend_on_record() {
        let snapshot = snapshot().with_bucket_rules(vec![rule("archive")]);
        let mut operation = CreateMultipartUploadOperation::new(input(snapshot));
        operation.start();

        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected one record write, got {effects:?}")
        };
        let record = MultipartUpload::from_bytes(value.as_ref()).unwrap();
        assert_eq!(record.backend, BackendRef::Node("tape".to_string()));
        assert_eq!(record.storage_class.as_deref(), Some("archive"));
    }

    #[test]
    fn missing_class_pins_default() {
        // The pin records where the parts actually land, not what was asked.
        let snapshot = snapshot().with_bucket_rules(vec![rule("glacier")]);
        let mut operation = CreateMultipartUploadOperation::new(input(snapshot));
        operation.start();

        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected one record write, got {effects:?}")
        };
        let record = MultipartUpload::from_bytes(value.as_ref()).unwrap();
        assert_eq!(record.backend, BackendRef::Node("default".to_string()));
        assert_eq!(record.storage_class, None);
    }

    #[test]
    fn unknown_backend_aborts() {
        let snapshot = snapshot().with_bucket_rules(vec![StorageRoutingRule {
            key_prefix: String::new(),
            exact: false,
            target: RoutingTarget::Backend(BackendRef::Node("ghost".to_string())),
        }]);
        let mut operation = CreateMultipartUploadOperation::new(input(snapshot));
        operation.start();

        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(CreateMultipartUploadError::RoutingFailed(
                RoutingError::UnknownBackend(_)
            ))
        ));
    }
}
