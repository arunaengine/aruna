//! Commits group resource records only while the group's deletion fence is open.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::SecretBytes;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::GROUP_DELETE_KEYSPACE;
use aruna_core::node_vault::VaultEntry;
use aruna_core::operation::{Operation, TransactionMode, boxed_suboperation};
use aruna_core::rows::done;
use aruna_core::structs::identity::group_delete::{GroupWriteError, check_group_write};
use aruna_core::types::{GroupId, Key, TxnId, Value};

pub fn group_fence_key(group_id: GroupId) -> (String, Key) {
    (
        GROUP_DELETE_KEYSPACE.to_string(),
        group_id.to_bytes().into(),
    )
}

pub fn write_group_records(group_id: GroupId, writes: Vec<(String, Key, Value)>) -> Effect {
    Effect::SubOperation(boxed_suboperation(
        GroupWriteOperation::new(group_id, writes),
        |result| Event::SubOperation(SubOperationEvent::GroupWritten { result }),
    ))
}

/// Like [`write_group_records`], refused with [`GroupWriteError::Taken`] when `absent` exists;
/// the row is read in the write's transaction, so a concurrent writer of it conflicts.
pub fn write_group_new(
    group_id: GroupId,
    writes: Vec<(String, Key, Value)>,
    secret: Option<(VaultEntry, SecretBytes)>,
    absent: (String, Key),
) -> Effect {
    let mut operation = GroupWriteOperation::new(group_id, writes);
    operation.secret = secret;
    operation.absent = Some(absent);
    Effect::SubOperation(boxed_suboperation(operation, |result| {
        Event::SubOperation(SubOperationEvent::GroupWritten { result })
    }))
}

/// Like [`write_group_records`], and seals `secret` into the node vault in the same transaction.
pub fn write_group_secret(
    group_id: GroupId,
    writes: Vec<(String, Key, Value)>,
    entry: VaultEntry,
    secret: SecretBytes,
) -> Effect {
    Effect::SubOperation(boxed_suboperation(
        GroupWriteOperation::new(group_id, writes).with_secret(entry, secret),
        |result| Event::SubOperation(SubOperationEvent::GroupWritten { result }),
    ))
}

pub fn group_write_result(event: Event) -> Result<(), GroupWriteError> {
    match event {
        Event::SubOperation(SubOperationEvent::GroupWritten { result }) => result,
        _ => Err(GroupWriteError::Unexpected),
    }
}

pub fn read_group_record(group_id: GroupId, key_space: &str, key: Key, txn_id: TxnId) -> Effect {
    Effect::Storage(StorageEffect::BatchRead {
        reads: vec![(key_space.to_string(), key), group_fence_key(group_id)],
        txn_id: Some(txn_id),
    })
}

pub fn parse_group_record(event: Event) -> Result<Option<Value>, GroupWriteError> {
    match event {
        Event::Storage(StorageEvent::BatchReadResult { mut values }) if values.len() == 2 => {
            let (_, fence) = values.pop().ok_or(GroupWriteError::Unexpected)?;
            check_group_write(fence.as_deref())?;
            Ok(values.pop().ok_or(GroupWriteError::Unexpected)?.1)
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(GroupWriteError::Unexpected),
    }
}

/// What the group write waits for after its fence read.
#[derive(Debug, PartialEq)]
enum Stage {
    Read,
    Write,
    Secret,
}

/// Writes group resource rows, and optionally seals a secret into the node vault, in one write
/// transaction that also reads the group's deletion fence.
#[derive(Debug, PartialEq)]
pub struct GroupWriteOperation {
    group_id: GroupId,
    writes: Vec<(String, Key, Value)>,
    secret: Option<(VaultEntry, SecretBytes)>,
    absent: Option<(String, Key)>,
    stage: Stage,
}

impl GroupWriteOperation {
    pub fn new(group_id: GroupId, writes: Vec<(String, Key, Value)>) -> Self {
        Self {
            group_id,
            writes,
            secret: None,
            absent: None,
            stage: Stage::Read,
        }
    }

    /// Seals `secret` into the node vault after the record writes, before the commit.
    pub fn with_secret(mut self, entry: VaultEntry, secret: SecretBytes) -> Self {
        self.secret = Some((entry, secret));
        self
    }

    /// Checks the fence and the row that must be absent, then writes the records.
    fn admit(&mut self, values: Vec<Option<Value>>) -> Result<Next<()>, GroupWriteError> {
        let mut values = values.into_iter();
        check_group_write(values.next().flatten().as_deref())?;
        if values.next().flatten().is_some() {
            return Err(GroupWriteError::Taken);
        }
        self.stage = Stage::Write;
        Ok(Next::Call(IoCall::Write(std::mem::take(&mut self.writes))))
    }
}

impl Operation for GroupWriteOperation {
    type Output = ();
    type Error = GroupWriteError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, GroupWriteError> {
        let mut reads = vec![group_fence_key(self.group_id)];
        reads.extend(self.absent.clone());
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GroupWriteError> {
        match self.stage {
            Stage::Read => match result {
                IoResult::Values(values) => self.admit(values),
                _ => Err(GroupWriteError::Unexpected),
            },
            Stage::Write => {
                done(result)?;
                let Some((entry, secret)) = self.secret.take() else {
                    return Ok(Next::Done(()));
                };
                self.stage = Stage::Secret;
                // The runner puts the vault write into this operation's transaction.
                let seal = crate::node_vault::write_secret(entry, secret, None);
                Ok(Next::Call(IoCall::Effect(Box::new(seal))))
            }
            Stage::Secret => match result {
                IoResult::Event(event) => match *event {
                    Event::Storage(StorageEvent::WriteResult { .. }) => Ok(Next::Done(())),
                    _ => Err(GroupWriteError::Unexpected),
                },
                _ => Err(GroupWriteError::Unexpected),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn taken_row_refused() {
        // An open fence admits the write; an existing row it must create refuses it.
        let group_id = ulid::Ulid::from_bytes([1; 16]);
        let row = ("rows".to_string(), Key::from(b"a".to_vec()));
        let writes = vec![(row.0.clone(), row.1.clone(), Value::from(b"v".to_vec()))];
        let mut operation = GroupWriteOperation::new(group_id, writes.clone());
        operation.absent = Some(row.clone());
        let reads = vec![group_fence_key(group_id), row];
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(reads))));
        let taken = IoResult::Values(vec![None, Some(Value::from(b"x".to_vec()))]);
        assert_eq!(operation.next(taken), Err(GroupWriteError::Taken));

        let mut operation = GroupWriteOperation::new(group_id, writes.clone());
        assert_eq!(operation.transaction(), TransactionMode::Write);
        operation.first().unwrap();
        let open = operation.next(IoResult::Values(vec![None]));
        assert_eq!(open, Ok(Next::Call(IoCall::Write(writes))));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
    }
}
