//! Replaces a tenant backend's credentials and name while keeping its id and its store.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::create::{CreateBackendError, CreateBackendInput};
use super::validation::{check_identity, check_import_root, validate_backend_input};
use super::{backend_key, record_writes};
use crate::storage_read::StorageReadError;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::SecretBytes;
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::STORAGE_BACKEND_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};
use aruna_core::structs::storage::group_backend::{GroupStorage, GroupStorageSecret};
use aruna_core::types::{GroupId, Key, KeySpace, Value};
use std::time::SystemTime;
use ulid::Ulid;

/// Where a replacement stands before its write transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReplaceState {
    ReadRecord,
    ReadSecret,
    Probe,
    Write,
}

type ReplaceTurn = Result<Next<GroupStorage>, CreateBackendError>;

/// Changes a tenant backend's credentials and name in place. The id and the
/// store it names both survive, so the objects already stamped with this
/// backend keep resolving, and a disabled backend stays disabled.
#[derive(Debug, PartialEq)]
pub struct ReplaceBackendOperation {
    input: CreateBackendInput,
    backend_id: Ulid,
    record: Option<GroupStorage>,
    secret: Option<GroupStorageSecret>,
    /// The stored record while its secret is read for a change without keys.
    existing: Option<GroupStorage>,
    /// A change without keys never writes the vault, so a rotation meanwhile stays.
    keep_secret: bool,
    state: ReplaceState,
}

impl ReplaceBackendOperation {
    pub fn new(backend_id: Ulid, input: CreateBackendInput) -> Self {
        Self {
            input,
            backend_id,
            record: None,
            secret: None,
            existing: None,
            keep_secret: false,
            state: ReplaceState::ReadRecord,
        }
    }

    fn handle_existing(&mut self, value: Option<Value>) -> ReplaceTurn {
        // Allowed while disabled: a leaked credential must be replaceable
        // without first enabling the backend for writes again.
        let existing = stored_record(value, self.input.group_id)?;
        // A change without keys keeps the stored ones.
        if self.input.secret_config.is_empty() {
            self.keep_secret = true;
            self.existing = Some(existing);
            self.state = ReplaceState::ReadSecret;
            let entry = GroupStorageSecret::vault_entry(self.backend_id);
            let read = crate::node_vault::read_secret(entry, None);
            return Ok(Next::Call(IoCall::Effect(Box::new(read))));
        }
        self.prepare(existing)
    }

    fn handle_secret(&mut self, event: Event) -> ReplaceTurn {
        let entry = GroupStorageSecret::vault_entry(self.backend_id);
        let secret = crate::node_vault::parse_secret(event, entry, GroupStorageSecret::from_bytes);
        let secret = match secret {
            Ok(Some(secret)) => secret,
            Ok(None) => return Err(CreateBackendError::NotFound),
            Err(StorageReadError::Storage(error)) => return Err(error.into()),
            Err(StorageReadError::Conversion(error)) => return Err(error.into()),
        };
        let existing = self.existing.take().ok_or(CreateBackendError::Failed)?;
        self.input.secret_config = secret.secret_config;
        self.prepare(existing)
    }

    fn prepare(&mut self, existing: GroupStorage) -> ReplaceTurn {
        let normalized = validate_backend_input(
            &self.input.name,
            self.input.kind,
            &self.input.public_config,
            &self.input.secret_config,
        )?;
        check_identity(&existing, self.input.kind, &normalized.public)?;
        let (kind, roots) = (self.input.kind, &self.input.import_roots);
        check_import_root(kind, &normalized.public, existing.group_id, roots)?;

        let now = SystemTime::now();
        let record = GroupStorage {
            backend_id: self.backend_id,
            group_id: existing.group_id,
            name: self.input.name.trim().to_string(),
            kind: self.input.kind,
            public_config: normalized.public,
            created_at: existing.created_at,
            updated_at: now,
            created_by: existing.created_by,
            disabled: existing.disabled,
            cleanup: self.input.cleanup,
        };
        let secret = GroupStorageSecret {
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
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    fn handle_probe(&mut self, event: Event) -> ReplaceTurn {
        match event {
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
        self.state = ReplaceState::Write;
        Ok(Next::Call(IoCall::child(self.write()?)))
    }

    /// The write transaction of the probed record, with the new keys unless they are kept.
    fn write(&self) -> Result<ReplaceWrite, CreateBackendError> {
        let record = self.record.clone().ok_or(CreateBackendError::Failed)?;
        let secret = match (self.keep_secret, self.secret.as_ref()) {
            (true, _) => None,
            (false, Some(secret)) => Some(secret.to_secret()?),
            (false, None) => return Err(CreateBackendError::Failed),
        };
        Ok(ReplaceWrite {
            group_id: self.input.group_id,
            record,
            secret,
            stage: WriteStage::Verify,
        })
    }
}

/// The stored backend record of `group_id`; a missing one or another group's is not found.
fn stored_record(
    value: Option<Value>,
    group_id: GroupId,
) -> Result<GroupStorage, CreateBackendError> {
    value
        .map(|value| GroupStorage::from_bytes(value.as_ref()))
        .transpose()?
        .filter(|record| record.group_id == group_id)
        .ok_or(CreateBackendError::NotFound)
}

/// The answer to an effect call, which the runner hands back as its event.
fn event(result: IoResult) -> Result<Event, CreateBackendError> {
    match result {
        IoResult::Event(event) => Ok(*event),
        _ => Err(CreateBackendError::Failed),
    }
}

impl Operation for ReplaceBackendOperation {
    type Output = GroupStorage;
    type Error = CreateBackendError;

    fn first(&mut self) -> ReplaceTurn {
        Ok(Next::Call(IoCall::Read(vec![record_row(self.backend_id)])))
    }

    fn next(&mut self, result: IoResult) -> ReplaceTurn {
        match self.state {
            ReplaceState::ReadRecord => {
                let [value] = values::<1>(result)?;
                self.handle_existing(value)
            }
            ReplaceState::ReadSecret => self.handle_secret(event(result)?),
            ReplaceState::Probe => self.handle_probe(event(result)?),
            ReplaceState::Write => result
                .child::<GroupStorage, CreateBackendError>()?
                .map(Next::Done),
        }
    }
}

fn record_row(backend_id: Ulid) -> (KeySpace, Key) {
    (
        STORAGE_BACKEND_KEYSPACE.to_string(),
        backend_key(backend_id),
    )
}

/// Where the replacement's write transaction stands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteStage {
    Verify,
    Records,
    Secret,
}

/// Re-reads the record, then writes it and the new keys in one transaction.
#[derive(Debug, PartialEq)]
struct ReplaceWrite {
    group_id: GroupId,
    record: GroupStorage,
    secret: Option<SecretBytes>,
    stage: WriteStage,
}

impl Operation for ReplaceWrite {
    type Output = GroupStorage;
    type Error = CreateBackendError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> ReplaceTurn {
        Ok(Next::Call(IoCall::Read(vec![record_row(
            self.record.backend_id,
        )])))
    }

    /// Reading the record again inside the write transaction serializes the
    /// update against a concurrent disable, which the probe is too slow to see.
    fn next(&mut self, result: IoResult) -> ReplaceTurn {
        match self.stage {
            WriteStage::Verify => {
                let [value] = values::<1>(result)?;
                let verified = stored_record(value, self.group_id)?;
                self.record.disabled = verified.disabled;
                self.record.public_config.extend(verified.managed_config());
                self.stage = WriteStage::Records;
                Ok(Next::Call(IoCall::Write(record_writes(&self.record)?)))
            }
            WriteStage::Records => {
                done(result)?;
                let Some(secret) = self.secret.take() else {
                    return Ok(Next::Done(self.record.clone()));
                };
                self.stage = WriteStage::Secret;
                // The runner puts the vault write into this operation's transaction.
                let entry = GroupStorageSecret::vault_entry(self.record.backend_id);
                let seal = crate::node_vault::write_secret(entry, secret, None);
                Ok(Next::Call(IoCall::Effect(Box::new(seal))))
            }
            WriteStage::Secret => match event(result)? {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    Ok(Next::Done(self.record.clone()))
                }
                Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
                _ => Err(CreateBackendError::Failed),
            },
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::ReplaceBackendOperation;
    use crate::groups::backends::create::{CreateBackendError, CreateBackendInput};
    use crate::groups::backends::validation::GroupBackendError;
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::operation::{Operation, TransactionMode};
    use aruna_core::structs::storage::cleanup::CleanupStrategy;
    use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
    use aruna_core::types::{Key, Value};
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime};
    use ulid::Ulid;

    fn input(group_id: Ulid) -> CreateBackendInput {
        CreateBackendInput {
            group_id,
            created_by: aruna_core::UserId::default(),
            name: "updated".to_string(),
            kind: GroupBackendKind::S3,
            public_config: HashMap::from([
                ("endpoint".to_string(), "https://s3.example.com".to_string()),
                ("bucket".to_string(), "data".to_string()),
            ]),
            secret_config: HashMap::from([
                ("access_key_id".to_string(), "new-id".to_string()),
                ("secret_access_key".to_string(), "new-key".to_string()),
            ]),
            cleanup: CleanupStrategy::Retain,
            import_roots: Default::default(),
        }
    }

    fn existing(group_id: Ulid, backend_id: Ulid) -> GroupStorage {
        GroupStorage {
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
            disabled: false,
            cleanup: CleanupStrategy::Retain,
        }
    }

    fn read(stored: &GroupStorage) -> IoResult {
        IoResult::Values(vec![Some(stored.to_bytes().unwrap().into())])
    }

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    fn refuse(input: CreateBackendInput) -> CreateBackendError {
        let group_id = input.group_id;
        let backend_id = Ulid::from_bytes([9u8; 16]);
        let mut operation = ReplaceBackendOperation::new(backend_id, input);
        operation.first().unwrap();
        operation
            .next(read(&existing(group_id, backend_id)))
            .expect_err("expected no probe")
    }

    /// Answers the probe and runs the write transaction against the stored record; returns the
    /// record writes and the output.
    fn commit_update(
        operation: &mut ReplaceBackendOperation,
        stored: GroupStorage,
    ) -> (Vec<(String, Key, Value)>, GroupStorage) {
        let checked = operation.next(event(Event::Blob(BlobEvent::GroupBackendChecked)));
        assert!(matches!(checked, Ok(Next::Call(IoCall::Child(_)))));
        let mut write = operation.write().unwrap();
        assert_eq!(write.transaction(), TransactionMode::Write);
        write.first().unwrap();
        let Ok(Next::Call(IoCall::Write(writes))) = write.next(read(&stored)) else {
            panic!("expected the record writes");
        };
        let seal = write.next(IoResult::Done);
        assert!(!format!("{seal:?} {operation:?} {write:?}").contains("new-key"));
        let Ok(Next::Call(IoCall::Effect(seal))) = seal else {
            panic!("expected the vault write");
        };
        assert!(matches!(
            *seal,
            Effect::Storage(StorageEffect::VaultWrite { txn_id: None, .. })
        ));
        let sealed = Event::Storage(StorageEvent::WriteResult {
            key: b"x".to_vec().into(),
        });
        let Ok(Next::Done(record)) = write.next(event(sealed)) else {
            panic!("expected the stored record");
        };
        (writes, record)
    }

    #[test]
    fn keeps_stored_keys() {
        // A change of name and public fields without keys keeps the stored keys and the
        // config Aruna manages.
        use aruna_core::structs::storage::group_backend::{DEFAULT_BUCKET, GroupStorageSecret};
        let (group_id, backend_id) = (Ulid::from_bytes([1u8; 16]), Ulid::from_bytes([9u8; 16]));
        let mut stored = existing(group_id, backend_id);
        stored.public_config.remove("bucket");
        for (key, value) in [
            (DEFAULT_BUCKET, "aruna-lab"),
            ("bucket_template", "aruna-{bucket}"),
            ("bucket_prefix", "lab-"),
        ] {
            stored
                .public_config
                .insert(key.to_string(), value.to_string());
        }
        let mut change = input(group_id);
        change.public_config.remove("bucket");
        change.secret_config.clear();
        let mut operation = ReplaceBackendOperation::new(backend_id, change);
        operation.first().unwrap();
        let entry = GroupStorageSecret::vault_entry(backend_id);
        let Ok(Next::Call(IoCall::Effect(read_secret))) = operation.next(read(&stored)) else {
            panic!("expected the vault read");
        };
        assert!(matches!(
            *read_secret,
            Effect::Storage(StorageEffect::VaultRead { entry: read, txn_id: None }) if read == entry
        ));
        let secret = GroupStorageSecret {
            backend_id,
            secret_config: HashMap::from([
                ("access_key_id".to_string(), "old-id".to_string()),
                ("secret_access_key".to_string(), "old-key".to_string()),
            ]),
            updated_at: SystemTime::UNIX_EPOCH,
        };
        let opened = event(Event::Storage(StorageEvent::VaultResult {
            entry,
            secret: Some(secret.to_secret().unwrap()),
        }));
        let Ok(Next::Call(IoCall::Effect(probe))) = operation.next(opened) else {
            panic!("expected one probe");
        };
        let Effect::Blob(BlobEffect::CheckGroupBackend { secret: probed, .. }) = *probe else {
            panic!("expected one probe");
        };
        assert_eq!(probed.secret_config, secret.secret_config);
        operation
            .next(event(Event::Blob(BlobEvent::GroupBackendChecked)))
            .unwrap();
        // The stored keys are only read: the record commits without a vault write.
        let mut write = operation.write().unwrap();
        write.first().unwrap();
        write.next(read(&stored)).unwrap();
        let Ok(Next::Done(record)) = write.next(IoResult::Done) else {
            panic!("expected the record without a vault write");
        };
        assert_eq!(record.name, "updated");
        assert_eq!(record.default_bucket(), Some("aruna-lab"));
        let config = &record.public_config;
        assert_eq!(
            config.get("bucket_template").map(String::as_str),
            Some("aruna-{bucket}")
        );
        assert_eq!(
            config.get("bucket_prefix").map(String::as_str),
            Some("lab-")
        );
    }

    #[test]
    fn keeps_backend_identity() {
        // An update must not mint a new id: stamped objects reference this one.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let backend_id = Ulid::from_bytes([9u8; 16]);
        let mut operation = ReplaceBackendOperation::new(backend_id, input(group_id));
        operation.first().unwrap();

        let Ok(Next::Call(IoCall::Effect(probe))) =
            operation.next(read(&existing(group_id, backend_id)))
        else {
            panic!("expected one probe");
        };
        let Effect::Blob(BlobEffect::CheckGroupBackend { record, .. }) = *probe else {
            panic!("expected one probe");
        };
        assert_eq!(record.backend_id, backend_id);
        assert_eq!(record.created_at, SystemTime::UNIX_EPOCH);
        assert!(record.updated_at > SystemTime::UNIX_EPOCH + Duration::from_secs(1));

        let (writes, record) = commit_update(&mut operation, existing(group_id, backend_id));
        assert!(!writes.is_empty());
        assert_eq!(record.backend_id, backend_id);
        assert_eq!(record.name, "updated");
    }

    #[test]
    fn update_keeps_disabled() {
        // A leaked credential must be replaceable without enabling writes again.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let backend_id = Ulid::from_bytes([9u8; 16]);
        let mut stored = existing(group_id, backend_id);
        stored.disabled = true;
        let mut operation = ReplaceBackendOperation::new(backend_id, input(group_id));
        operation.first().unwrap();
        let probe = operation.next(read(&stored));
        assert!(matches!(probe, Ok(Next::Call(IoCall::Effect(_)))));

        let (_, record) = commit_update(&mut operation, stored);
        assert!(record.disabled);
    }

    #[test]
    fn update_keeps_managed() {
        // The default bucket comes from the record read in the transaction, not the request.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let backend_id = Ulid::from_bytes([9u8; 16]);
        let mut stored = existing(group_id, backend_id);
        stored
            .public_config
            .insert("default_bucket".to_string(), "lab-default".to_string());
        let mut operation = ReplaceBackendOperation::new(backend_id, input(group_id));
        operation.first().unwrap();
        operation
            .next(read(&existing(group_id, backend_id)))
            .unwrap();

        let (_, record) = commit_update(&mut operation, stored);
        assert_eq!(record.default_bucket(), Some("lab-default"));
    }

    #[test]
    fn refuses_moved_store() {
        // Stored locations carry neither kind nor endpoint, and record only the
        // path below `root`, so all three are frozen.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut moved = input(group_id);
        moved
            .public_config
            .insert("bucket".to_string(), "elsewhere".to_string());
        assert_eq!(
            refuse(moved),
            CreateBackendError::Invalid(GroupBackendError::Immutable("bucket".to_string()))
        );

        let mut rerooted = input(group_id);
        rerooted
            .public_config
            .insert("root".to_string(), "moved".to_string());
        assert_eq!(
            refuse(rerooted),
            CreateBackendError::Invalid(GroupBackendError::Immutable("root".to_string()))
        );

        let mut retyped = input(group_id);
        retyped.kind = GroupBackendKind::Gcs;
        retyped.public_config = HashMap::from([("bucket".to_string(), "data".to_string())]);
        retyped.secret_config = HashMap::from([("credential".to_string(), "token".to_string())]);
        assert_eq!(
            refuse(retyped),
            CreateBackendError::Invalid(GroupBackendError::Immutable("type".to_string()))
        );
    }

    #[test]
    fn rejects_foreign_group() {
        let backend_id = Ulid::from_bytes([9u8; 16]);
        let mut operation =
            ReplaceBackendOperation::new(backend_id, input(Ulid::from_bytes([1u8; 16])));
        operation.first().unwrap();
        let foreign = read(&existing(Ulid::from_bytes([2u8; 16]), backend_id));
        assert_eq!(
            operation.next(foreign).err(),
            Some(CreateBackendError::NotFound)
        );
    }
}
