//! Registers a new tenant storage backend for a group after probing its credentials.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::validation::{GroupBackendError, check_import_root, validate_backend_input};
use super::{RecordReadError, record_writes};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::group_delete::GroupWriteError;
use aruna_core::structs::storage::backends::ImportRoot;
use aruna_core::structs::storage::cleanup::CleanupStrategy;
use aruna_core::structs::storage::group_backend::{
    GroupBackendKind, GroupStorage, GroupStorageSecret,
};
use aruna_core::types::GroupId;
use std::collections::{BTreeMap, HashMap};
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

use crate::groups::fence::GroupWriteOperation;

#[derive(Clone, PartialEq, Eq)]
pub struct CreateBackendInput {
    pub group_id: GroupId,
    pub created_by: UserId,
    pub name: String,
    pub kind: GroupBackendKind,
    pub public_config: HashMap<String, String>,
    pub secret_config: HashMap<String, String>,
    pub cleanup: CleanupStrategy,
    /// This node's import roots, which a local folder backend must name.
    pub import_roots: BTreeMap<String, ImportRoot>,
}

/// Shows only the secret keys; the values are live credentials.
impl std::fmt::Debug for CreateBackendInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateBackendInput")
            .field("group_id", &self.group_id)
            .field("created_by", &self.created_by)
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("public_config", &self.public_config)
            .field(
                "secret_keys",
                &self.secret_config.keys().collect::<Vec<_>>(),
            )
            .field("cleanup", &self.cleanup)
            .field("import_roots", &self.import_roots)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreateState {
    Probe,
    WriteRecords,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateBackendError {
    #[error(transparent)]
    GroupWrite(#[from] aruna_core::structs::identity::group_delete::GroupWriteError),
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

impl From<IoError> for CreateBackendError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            _ => Self::Failed,
        }
    }
}

type CreateTurn = Result<Next<GroupStorage>, CreateBackendError>;

/// Registers a tenant write backend. The credentials are proved against the
/// endpoint before either record is stored, so a broken backend never lands.
#[derive(Debug, PartialEq)]
pub struct CreateBackendOperation {
    input: CreateBackendInput,
    state: CreateState,
    record: Option<GroupStorage>,
    secret: Option<GroupStorageSecret>,
}

impl CreateBackendOperation {
    pub fn new(input: CreateBackendInput) -> Self {
        Self {
            input,
            state: CreateState::Probe,
            record: None,
            secret: None,
        }
    }

    /// The guarded write of both records, with the secret sealed into the node vault.
    fn group_write(&self) -> Result<GroupWriteOperation, CreateBackendError> {
        let (Some(record), Some(secret)) = (self.record.as_ref(), self.secret.as_ref()) else {
            return Err(CreateBackendError::Failed);
        };
        let writes = record_writes(record)?;
        let entry = GroupStorageSecret::vault_entry(record.backend_id);
        let write = GroupWriteOperation::new(self.input.group_id, writes);
        Ok(write.with_secret(entry, secret.to_secret()?))
    }
}

impl Operation for CreateBackendOperation {
    type Output = GroupStorage;
    type Error = CreateBackendError;

    fn first(&mut self) -> CreateTurn {
        let normalized = validate_backend_input(
            &self.input.name,
            self.input.kind,
            &self.input.public_config,
            &self.input.secret_config,
        )?;
        let (kind, roots) = (self.input.kind, &self.input.import_roots);
        check_import_root(kind, &normalized.public, self.input.group_id, roots)?;

        let now = SystemTime::now();
        let backend_id = Ulid::generate();
        let record = GroupStorage {
            backend_id,
            group_id: self.input.group_id,
            name: self.input.name.trim().to_string(),
            kind: self.input.kind,
            public_config: normalized.public,
            created_at: now,
            updated_at: now,
            created_by: self.input.created_by,
            disabled: false,
            cleanup: self.input.cleanup,
        };
        let secret = GroupStorageSecret {
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
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    fn next(&mut self, result: IoResult) -> CreateTurn {
        match self.state {
            CreateState::Probe => {
                let IoResult::Event(event) = result else {
                    return Err(CreateBackendError::Failed);
                };
                match *event {
                    Event::Blob(BlobEvent::GroupBackendChecked) => {}
                    Event::Blob(BlobEvent::Error(error)) => {
                        return Err(CreateBackendError::Unreachable(error));
                    }
                    received => {
                        return Err(CreateBackendError::InvalidStateEvent {
                            state: "Probe",
                            expected: "Event::Blob(BlobEvent::GroupBackendChecked)",
                            received,
                        });
                    }
                }
                self.state = CreateState::WriteRecords;
                Ok(Next::Call(IoCall::child(self.group_write()?)))
            }
            CreateState::WriteRecords => {
                result.child::<(), GroupWriteError>()??;
                let record = self.record.clone().ok_or(CreateBackendError::Failed)?;
                Ok(Next::Done(record))
            }
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{CreateBackendError, CreateBackendInput, CreateBackendOperation};
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::errors::BlobError;
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::keyspaces::{BACKEND_INDEX_KEYSPACE, STORAGE_BACKEND_KEYSPACE};
    use aruna_core::operation::Operation;
    use aruna_core::structs::storage::cleanup::CleanupStrategy;
    use aruna_core::structs::storage::group_backend::{
        GroupBackendKind, GroupStorage, GroupStorageSecret,
    };
    use std::collections::HashMap;
    use ulid::Ulid;

    fn input() -> CreateBackendInput {
        CreateBackendInput {
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
                ("secret_access_key".to_string(), "canary-2f9b".to_string()),
            ]),
            cleanup: CleanupStrategy::Retain,
            import_roots: Default::default(),
        }
    }

    fn probed() -> CreateBackendOperation {
        let mut operation = CreateBackendOperation::new(input());
        operation.first().unwrap();
        operation
    }

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    #[test]
    fn probes_before_writing() {
        // A backend that cannot be written to must never reach storage.
        let mut operation = CreateBackendOperation::new(input());

        let probe = operation.first();
        let mut formatted = vec![format!("{probe:?} {operation:?}")];
        let Ok(Next::Call(IoCall::Effect(effect))) = probe else {
            panic!("expected the probe");
        };
        assert!(matches!(
            *effect,
            Effect::Blob(BlobEffect::CheckGroupBackend { .. })
        ));

        let written = operation.next(event(Event::Blob(BlobEvent::GroupBackendChecked)));
        formatted.push(format!("{written:?} {operation:?}"));
        assert!(matches!(written, Ok(Next::Call(IoCall::Child(_)))));
        let mut write = operation.group_write().unwrap();
        write.first().unwrap();
        let Ok(Next::Call(IoCall::Write(writes))) = write.next(IoResult::Values(vec![None])) else {
            panic!("expected one batch write");
        };
        assert_eq!(
            writes
                .iter()
                .map(|(key_space, ..)| key_space.as_str())
                .collect::<Vec<_>>(),
            [STORAGE_BACKEND_KEYSPACE, BACKEND_INDEX_KEYSPACE]
        );
        let stored = GroupStorage::from_bytes(writes[0].2.as_ref()).unwrap();
        assert!(!stored.public_config.contains_key("access_key_id"));

        // The secret goes into the node vault inside the same transaction.
        let seal = write.next(IoResult::Done);
        formatted.push(format!("{seal:?} {write:?}"));
        let Ok(Next::Call(IoCall::Effect(seal))) = seal else {
            panic!("expected a vault write");
        };
        let Effect::Storage(StorageEffect::VaultWrite { entry, txn_id, .. }) = *seal else {
            panic!("expected a vault write");
        };
        assert_eq!(entry, GroupStorageSecret::vault_entry(stored.backend_id));
        assert_eq!(txn_id, None);
        for text in formatted {
            assert!(!text.contains("canary"), "{text}");
        }
    }

    #[test]
    fn probe_failure_aborts() {
        let failed = Event::Blob(BlobEvent::Error(BlobError::WriteError(
            "denied".to_string(),
        )));
        assert!(matches!(
            probed().next(event(failed)),
            Err(CreateBackendError::Unreachable(_))
        ));
    }

    #[test]
    fn rejects_bad_config() {
        let mut input = input();
        input.secret_config.remove("secret_access_key");
        let mut operation = CreateBackendOperation::new(input);
        assert!(matches!(
            operation.first(),
            Err(CreateBackendError::Invalid(_))
        ));
    }

    #[test]
    fn rejects_unexpected_event() {
        let other = Event::Storage(StorageEvent::WriteResult {
            key: b"x".to_vec().into(),
        });
        assert!(matches!(
            probed().next(event(other)),
            Err(CreateBackendError::InvalidStateEvent { .. })
        ));
    }
}
