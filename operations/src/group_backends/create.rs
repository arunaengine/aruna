use super::validation::{GroupBackendError, validate_backend_input};
use super::{RecordReadError, backend_key};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    GROUP_STORAGE_BACKEND_KEYSPACE, GROUP_STORAGE_BACKEND_SECRET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{GroupBackendKind, GroupStorageBackend, GroupStorageBackendSecret};
use aruna_core::types::{Effects, GroupId, UserId};
use smallvec::smallvec;
use std::collections::HashMap;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateGroupBackendInput {
    pub group_id: GroupId,
    pub created_by: UserId,
    pub name: String,
    pub kind: GroupBackendKind,
    pub public_config: HashMap<String, String>,
    pub secret_config: HashMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CreateState {
    Init,
    Probe,
    WriteRecords,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateGroupBackendError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    Invalid(#[from] GroupBackendError),
    #[error(transparent)]
    Read(#[from] RecordReadError),
    #[error("storage backend not found")]
    NotFound,
    #[error("backend is not usable: {0}")]
    Unreachable(#[from] BlobError),
    #[error("CreateGroupBackend failed")]
    Failed,
    #[error("State [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

/// Registers a tenant write backend. The credentials are proved against the
/// endpoint before either record is stored, so a broken backend never lands.
#[derive(Debug, PartialEq)]
pub struct CreateGroupBackendOperation {
    input: CreateGroupBackendInput,
    state: CreateState,
    record: Option<GroupStorageBackend>,
    secret: Option<GroupStorageBackendSecret>,
    output: Option<Result<GroupStorageBackend, CreateGroupBackendError>>,
}

impl CreateGroupBackendOperation {
    pub fn new(input: CreateGroupBackendInput) -> Self {
        Self {
            input,
            state: CreateState::Init,
            record: None,
            secret: None,
            output: None,
        }
    }

    fn fail(&mut self, error: CreateGroupBackendError) -> Effects {
        self.state = CreateState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        let normalized = match validate_backend_input(
            &self.input.name,
            self.input.kind,
            &self.input.public_config,
            &self.input.secret_config,
        ) {
            Ok(normalized) => normalized,
            Err(error) => return self.fail(error.into()),
        };

        let now = SystemTime::now();
        let backend_id = Ulid::generate();
        let record = GroupStorageBackend {
            backend_id,
            group_id: self.input.group_id,
            name: self.input.name.trim().to_string(),
            kind: self.input.kind,
            public_config: normalized.public,
            created_at: now,
            updated_at: now,
            created_by: self.input.created_by,
        };
        let secret = GroupStorageBackendSecret {
            backend_id,
            secret_config: normalized.secret,
            updated_at: now,
        };

        self.state = CreateState::Probe;
        let effect = Effect::Blob(BlobEffect::CheckGroupBackend {
            record: record.clone(),
            secret: secret.clone(),
        });
        self.record = Some(record);
        self.secret = Some(secret);
        smallvec![effect]
    }

    fn handle_probe(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::GroupBackendChecked) => {}
            Event::Blob(BlobEvent::Error(error)) => {
                return self.fail(CreateGroupBackendError::Unreachable(error));
            }
            received => {
                return self.fail(CreateGroupBackendError::InvalidStateEvent {
                    state: "Probe",
                    expected: "Event::Blob(BlobEvent::GroupBackendChecked)",
                    received,
                });
            }
        }

        let (Some(record), Some(secret)) = (self.record.as_ref(), self.secret.as_ref()) else {
            return self.fail(CreateGroupBackendError::Failed);
        };
        let record_bytes = match record.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error.into()),
        };
        let secret_bytes = match secret.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error.into()),
        };

        self.state = CreateState::WriteRecords;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (
                    GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
                    backend_key(record.backend_id),
                    record_bytes.into(),
                ),
                (
                    GROUP_STORAGE_BACKEND_SECRET_KEYSPACE.to_string(),
                    backend_key(record.backend_id),
                    secret_bytes.into(),
                ),
            ],
            txn_id: None,
        })]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.fail(CreateGroupBackendError::InvalidStateEvent {
                state: "WriteRecords",
                expected: "Event::Storage(StorageEvent::BatchWriteResult)",
                received: event,
            });
        };
        let Some(record) = self.record.clone() else {
            return self.fail(CreateGroupBackendError::Failed);
        };
        self.state = CreateState::Finish;
        self.output = Some(Ok(record));
        smallvec![]
    }
}

impl Operation for CreateGroupBackendOperation {
    type Output = GroupStorageBackend;
    type Error = CreateGroupBackendError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            CreateState::Init => self.handle_init(),
            CreateState::Probe => self.handle_probe(event),
            CreateState::WriteRecords => self.handle_written(event),
            CreateState::Finish | CreateState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, CreateState::Finish | CreateState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(result) => result,
            None => Err(CreateGroupBackendError::Failed),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{CreateGroupBackendError, CreateGroupBackendInput, CreateGroupBackendOperation};
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::errors::BlobError;
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::keyspaces::{
        GROUP_STORAGE_BACKEND_KEYSPACE, GROUP_STORAGE_BACKEND_SECRET_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::structs::{GroupBackendKind, GroupStorageBackend};
    use std::collections::HashMap;
    use ulid::Ulid;

    fn input() -> CreateGroupBackendInput {
        CreateGroupBackendInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            created_by: aruna_core::UserId::default(),
            name: "tenant".to_string(),
            kind: GroupBackendKind::S3,
            public_config: HashMap::from([
                ("endpoint".to_string(), "https://s3.example.com".to_string()),
                ("bucket".to_string(), "data".to_string()),
            ]),
            secret_config: HashMap::from([
                ("access_key_id".to_string(), "id".to_string()),
                ("secret_access_key".to_string(), "key".to_string()),
            ]),
        }
    }

    #[test]
    fn probes_before_writing() {
        // A backend that cannot be written to must never reach storage.
        let mut operation = CreateGroupBackendOperation::new(input());

        let effects = operation.start();

        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::CheckGroupBackend { .. })]
        ));

        let effects = operation.step(Event::Blob(BlobEvent::GroupBackendChecked));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected one batch write, got {effects:?}")
        };
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0].0, GROUP_STORAGE_BACKEND_KEYSPACE);
        assert_eq!(writes[1].0, GROUP_STORAGE_BACKEND_SECRET_KEYSPACE);
        assert_eq!(writes[0].1, writes[1].1);
        let stored = GroupStorageBackend::from_bytes(writes[0].2.as_ref()).unwrap();
        assert!(!stored.public_config.contains_key("access_key_id"));
    }

    #[test]
    fn probe_failure_aborts() {
        let mut operation = CreateGroupBackendOperation::new(input());
        operation.start();

        let effects = operation.step(Event::Blob(BlobEvent::Error(BlobError::WriteError(
            "denied".to_string(),
        ))));

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(CreateGroupBackendError::Unreachable(_))
        ));
    }

    #[test]
    fn rejects_bad_config() {
        let mut input = input();
        input.secret_config.remove("secret_access_key");
        let mut operation = CreateGroupBackendOperation::new(input);

        let effects = operation.start();

        assert!(effects.is_empty());
        assert!(matches!(
            operation.finalize(),
            Err(CreateGroupBackendError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_unexpected_event() {
        let mut operation = CreateGroupBackendOperation::new(input());
        operation.start();

        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: b"x".to_vec().into(),
        }));

        assert!(matches!(
            operation.finalize(),
            Err(CreateGroupBackendError::InvalidStateEvent { .. })
        ));
    }
}
