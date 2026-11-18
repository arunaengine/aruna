//! Sets or clears the disabled flag on one group storage backend record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{backend_key, record_writes};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::STORAGE_BACKEND_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::types::GroupId;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error, PartialEq)]
pub enum SetDisabledError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("storage backend not found")]
    NotFound,
    #[error("SetDisabled failed")]
    Failed,
}

impl From<IoError> for SetDisabledError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::StorageError(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::StorageError(error),
            _ => Self::Failed,
        }
    }
}

/// Turns a tenant backend's writes off or back on. Reads keep working either
/// way: the record and its credentials stay, so objects already stored on the
/// backend still resolve and queued cleanups still reach it.
#[derive(Debug, PartialEq)]
pub struct SetDisabledOperation {
    group_id: GroupId,
    backend_id: Ulid,
    disabled: bool,
    updated: Option<GroupStorage>,
}

impl SetDisabledOperation {
    pub fn new(group_id: GroupId, backend_id: Ulid, disabled: bool) -> Self {
        Self {
            group_id,
            backend_id,
            disabled,
            updated: None,
        }
    }

    /// The key carries no group, so the record's own group is the gate. An
    /// unchanged flag writes nothing, which makes repeating the call harmless.
    fn handle_record(
        &mut self,
        value: Option<aruna_core::types::Value>,
    ) -> Result<Next<GroupStorage>, SetDisabledError> {
        let record = value
            .map(|value| GroupStorage::from_bytes(value.as_ref()))
            .transpose()?
            .filter(|record| record.group_id == self.group_id)
            .ok_or(SetDisabledError::NotFound)?;
        if record.disabled == self.disabled {
            return Ok(Next::Done(record));
        }

        // The stamp is what removal waits on: a writer that resolved the
        // backend just before this commit needs its credentials to survive.
        let updated = GroupStorage {
            disabled: self.disabled,
            updated_at: SystemTime::now(),
            ..record
        };
        let writes = record_writes(&updated)?;
        self.updated = Some(updated);
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for SetDisabledOperation {
    type Output = GroupStorage;
    type Error = SetDisabledError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<GroupStorage>, SetDisabledError> {
        let row = (
            STORAGE_BACKEND_KEYSPACE.to_string(),
            backend_key(self.backend_id),
        );
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<GroupStorage>, SetDisabledError> {
        let Some(updated) = self.updated.take() else {
            let [value] = values::<1>(result)?;
            return self.handle_record(value);
        };
        done(result)?;
        Ok(Next::Done(updated))
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{SetDisabledError, SetDisabledOperation};
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::keyspaces::{BACKEND_INDEX_KEYSPACE, STORAGE_BACKEND_KEYSPACE};
    use aruna_core::operation::{Operation, TransactionMode};
    use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn backend_id() -> Ulid {
        Ulid::from_bytes([9u8; 16])
    }

    fn record(group_id: Ulid, disabled: bool) -> GroupStorage {
        GroupStorage {
            backend_id: backend_id(),
            group_id,
            name: "tenant".to_string(),
            kind: GroupBackendKind::Gcs,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            disabled,
            cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
        }
    }

    /// The operation after its record read went out, inside its transaction.
    fn reading(group_id: Ulid, disabled: bool) -> SetDisabledOperation {
        let mut operation = SetDisabledOperation::new(group_id, backend_id(), disabled);
        assert_eq!(operation.transaction(), TransactionMode::Write);
        operation.first().unwrap();
        operation
    }

    fn read_result(stored: GroupStorage) -> IoResult {
        IoResult::Values(vec![Some(stored.to_bytes().unwrap().into())])
    }

    /// Answers the write and returns the output.
    fn written(operation: &mut SetDisabledOperation) -> GroupStorage {
        match operation.next(IoResult::Done) {
            Ok(Next::Done(record)) => record,
            _ => panic!("expected the stored record"),
        }
    }

    #[test]
    fn disable_stamps_record() {
        // Removal waits on this stamp, so a disable has to move it.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut operation = reading(group_id, true);
        operation
            .next(read_result(record(group_id, false)))
            .unwrap();
        assert!(written(&mut operation).updated_at > SystemTime::UNIX_EPOCH);
    }

    #[test]
    fn disable_sets_flag() {
        // Both copies of the record must carry the flag, or routing disagrees.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut operation = reading(group_id, true);

        let Ok(Next::Call(IoCall::Write(writes))) =
            operation.next(read_result(record(group_id, false)))
        else {
            panic!("expected one write");
        };
        assert_eq!(
            writes
                .iter()
                .map(|(key_space, ..)| key_space.as_str())
                .collect::<Vec<_>>(),
            [STORAGE_BACKEND_KEYSPACE, BACKEND_INDEX_KEYSPACE]
        );
        assert!(
            writes
                .iter()
                .all(|(.., value)| { GroupStorage::from_bytes(value.as_ref()).unwrap().disabled })
        );
        assert!(written(&mut operation).disabled);
    }

    #[test]
    fn disable_is_idempotent() {
        // Repeating the call must succeed without writing anything.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut operation = reading(group_id, true);
        let unchanged = operation.next(read_result(record(group_id, true)));
        assert!(matches!(unchanged, Ok(Next::Done(record)) if record.disabled));
    }

    #[test]
    fn enable_clears_flag() {
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut operation = reading(group_id, false);
        operation.next(read_result(record(group_id, true))).unwrap();
        assert!(!written(&mut operation).disabled);
    }

    #[test]
    fn rejects_foreign_group() {
        let mut operation = reading(Ulid::from_bytes([1u8; 16]), true);
        let foreign = read_result(record(Ulid::from_bytes([2u8; 16]), false));
        assert_eq!(operation.next(foreign), Err(SetDisabledError::NotFound));
    }

    #[test]
    fn rejects_stray_answer() {
        // A write acknowledgement is not the record read.
        let mut operation = reading(Ulid::from_bytes([1u8; 16]), true);
        assert_eq!(
            operation.next(IoResult::Done),
            Err(SetDisabledError::Failed)
        );
    }
}
