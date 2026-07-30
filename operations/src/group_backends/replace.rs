use super::create::{CreateGroupBackendError, CreateGroupBackendInput};
use super::validation::{check_identity, validate_backend_input};
use super::{backend_key, parse_read};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    GROUP_STORAGE_BACKEND_KEYSPACE, GROUP_STORAGE_BACKEND_SECRET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{GroupStorageBackend, GroupStorageBackendSecret};
use aruna_core::types::Effects;
use smallvec::smallvec;
use std::time::SystemTime;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReplaceState {
    Init,
    ReadRecord,
    Probe,
    WriteRecords,
    Finish,
    Error,
}

/// Rotates a tenant backend's credentials and name in place. The id and the
/// store it names both survive, so the objects already stamped with this
/// backend keep resolving.
#[derive(Debug, PartialEq)]
pub struct ReplaceGroupBackendOperation {
    input: CreateGroupBackendInput,
    backend_id: Ulid,
    record: Option<GroupStorageBackend>,
    secret: Option<GroupStorageBackendSecret>,
    state: ReplaceState,
    output: Option<Result<GroupStorageBackend, CreateGroupBackendError>>,
}

impl ReplaceGroupBackendOperation {
    pub fn new(backend_id: Ulid, input: CreateGroupBackendInput) -> Self {
        Self {
            input,
            backend_id,
            record: None,
            secret: None,
            state: ReplaceState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: CreateGroupBackendError) -> Effects {
        self.state = ReplaceState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_existing(&mut self, event: Event) -> Effects {
        let existing = match parse_read(event, GroupStorageBackend::from_bytes) {
            Ok(Some(record)) if record.group_id == self.input.group_id => record,
            Ok(_) => return self.fail(CreateGroupBackendError::NotFound),
            Err(error) => return self.fail(CreateGroupBackendError::Read(error)),
        };

        let normalized = match validate_backend_input(
            &self.input.name,
            self.input.kind,
            &self.input.public_config,
            &self.input.secret_config,
        ) {
            Ok(normalized) => normalized,
            Err(error) => return self.fail(error.into()),
        };
        if let Err(error) = check_identity(&existing, self.input.kind, &normalized.public) {
            return self.fail(error.into());
        }

        let now = SystemTime::now();
        let record = GroupStorageBackend {
            backend_id: self.backend_id,
            group_id: existing.group_id,
            name: self.input.name.trim().to_string(),
            kind: self.input.kind,
            public_config: normalized.public,
            created_at: existing.created_at,
            updated_at: now,
            created_by: existing.created_by,
        };
        let secret = GroupStorageBackendSecret {
            backend_id: self.backend_id,
            secret_config: normalized.secret,
            updated_at: now,
        };

        self.state = ReplaceState::Probe;
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

        self.state = ReplaceState::WriteRecords;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (
                    GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
                    backend_key(self.backend_id),
                    record_bytes.into(),
                ),
                (
                    GROUP_STORAGE_BACKEND_SECRET_KEYSPACE.to_string(),
                    backend_key(self.backend_id),
                    secret_bytes.into(),
                ),
            ],
            txn_id: None,
        })]
    }
}

impl Operation for ReplaceGroupBackendOperation {
    type Output = GroupStorageBackend;
    type Error = CreateGroupBackendError;

    fn start(&mut self) -> Effects {
        self.state = ReplaceState::ReadRecord;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
            key: backend_key(self.backend_id),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReplaceState::Init => self.start(),
            ReplaceState::ReadRecord => self.handle_existing(event),
            ReplaceState::Probe => self.handle_probe(event),
            ReplaceState::WriteRecords => {
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
                self.state = ReplaceState::Finish;
                self.output = Some(Ok(record));
                smallvec![]
            }
            ReplaceState::Finish | ReplaceState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReplaceState::Finish | ReplaceState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(CreateGroupBackendError::Failed))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::ReplaceGroupBackendOperation;
    use crate::group_backends::create::{CreateGroupBackendError, CreateGroupBackendInput};
    use crate::group_backends::validation::GroupBackendError;
    use aruna_core::effects::{BlobEffect, Effect};
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{GroupBackendKind, GroupStorageBackend};
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime};
    use ulid::Ulid;

    fn input(group_id: Ulid) -> CreateGroupBackendInput {
        CreateGroupBackendInput {
            group_id,
            created_by: aruna_core::UserId::default(),
            name: "rotated".to_string(),
            kind: GroupBackendKind::S3,
            public_config: HashMap::from([
                ("endpoint".to_string(), "https://s3.example.com".to_string()),
                ("bucket".to_string(), "data".to_string()),
            ]),
            secret_config: HashMap::from([
                ("access_key_id".to_string(), "new-id".to_string()),
                ("secret_access_key".to_string(), "new-key".to_string()),
            ]),
        }
    }

    fn existing(group_id: Ulid, backend_id: Ulid) -> GroupStorageBackend {
        GroupStorageBackend {
            backend_id,
            group_id,
            name: "old".to_string(),
            kind: GroupBackendKind::S3,
            public_config: HashMap::from([
                ("endpoint".to_string(), "https://s3.example.com".to_string()),
                ("bucket".to_string(), "data".to_string()),
            ]),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
        }
    }

    fn refuse(input: CreateGroupBackendInput) -> CreateGroupBackendError {
        let group_id = input.group_id;
        let backend_id = Ulid::from_bytes([9u8; 16]);
        let mut operation = ReplaceGroupBackendOperation::new(backend_id, input);
        operation.start();

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(existing(group_id, backend_id).to_bytes().unwrap().into()),
        }));

        assert!(effects.is_empty(), "expected no probe, got {effects:?}");
        operation.finalize().unwrap_err()
    }

    #[test]
    fn keeps_backend_identity() {
        // Rotation must not mint a new id: stamped objects reference this one.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let backend_id = Ulid::from_bytes([9u8; 16]);
        let mut operation = ReplaceGroupBackendOperation::new(backend_id, input(group_id));
        operation.start();

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(existing(group_id, backend_id).to_bytes().unwrap().into()),
        }));

        let [Effect::Blob(BlobEffect::CheckGroupBackend { record, .. })] = effects.as_slice()
        else {
            panic!("expected one probe, got {effects:?}")
        };
        assert_eq!(record.backend_id, backend_id);
        assert_eq!(record.created_at, SystemTime::UNIX_EPOCH);
        assert!(record.updated_at > SystemTime::UNIX_EPOCH + Duration::from_secs(1));

        operation.step(Event::Blob(BlobEvent::GroupBackendChecked));
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert_eq!(operation.finalize().unwrap().name, "rotated");
    }

    #[test]
    fn refuses_moved_store() {
        // Stored locations carry neither kind nor endpoint, so both are frozen.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut moved = input(group_id);
        moved
            .public_config
            .insert("bucket".to_string(), "elsewhere".to_string());
        assert_eq!(
            refuse(moved),
            CreateGroupBackendError::Invalid(GroupBackendError::Immutable("bucket".to_string()))
        );

        let mut retyped = input(group_id);
        retyped.kind = GroupBackendKind::Gcs;
        retyped.public_config = HashMap::from([("bucket".to_string(), "data".to_string())]);
        retyped.secret_config = HashMap::from([("credential".to_string(), "token".to_string())]);
        assert_eq!(
            refuse(retyped),
            CreateGroupBackendError::Invalid(GroupBackendError::Immutable("type".to_string()))
        );
    }

    #[test]
    fn rejects_foreign_group() {
        let backend_id = Ulid::from_bytes([9u8; 16]);
        let mut operation =
            ReplaceGroupBackendOperation::new(backend_id, input(Ulid::from_bytes([1u8; 16])));
        operation.start();

        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(
                existing(Ulid::from_bytes([2u8; 16]), backend_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
        }));

        assert!(matches!(
            operation.finalize(),
            Err(CreateGroupBackendError::NotFound)
        ));
    }
}
