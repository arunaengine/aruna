//! Commits group resource records only while the group's deletion fence is open.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::SecretBytes;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::GROUP_DELETE_KEYSPACE;
use aruna_core::node_vault::VaultEntry;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::group_delete::{GroupWriteError, check_group_write};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use smallvec::smallvec;

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

#[derive(Debug, PartialEq)]
enum State {
    Init,
    Start,
    Read,
    Write,
    Secret,
    Commit,
    Finish,
}

#[derive(Debug, PartialEq)]
pub struct GroupWriteOperation {
    group_id: GroupId,
    writes: Vec<(String, Key, Value)>,
    secret: Option<(VaultEntry, SecretBytes)>,
    absent: Option<(String, Key)>,
    txn_id: Option<TxnId>,
    state: State,
    output: Option<Result<(), GroupWriteError>>,
}

impl GroupWriteOperation {
    pub fn new(group_id: GroupId, writes: Vec<(String, Key, Value)>) -> Self {
        Self {
            group_id,
            writes,
            secret: None,
            absent: None,
            txn_id: None,
            state: State::Init,
            output: None,
        }
    }

    /// Seals `secret` into the node vault after the record writes, before the commit.
    pub fn with_secret(mut self, entry: VaultEntry, secret: SecretBytes) -> Self {
        self.secret = Some((entry, secret));
        self
    }

    fn write(&mut self) -> Effects {
        self.state = State::Write;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: std::mem::take(&mut self.writes),
            txn_id: self.txn_id,
        })]
    }

    fn fail(&mut self, error: GroupWriteError) -> Effects {
        self.output = Some(Err(error));
        self.state = State::Finish;
        self.abort()
    }
}

impl Operation for GroupWriteOperation {
    type Output = ();
    type Error = GroupWriteError;

    fn start(&mut self) -> Effects {
        self.state = State::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (&self.state, event) {
            (State::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = State::Read;
                let (key_space, key) = group_fence_key(self.group_id);
                let Some(absent) = self.absent.clone() else {
                    return smallvec![Effect::Storage(StorageEffect::Read {
                        key_space,
                        key,
                        txn_id: Some(txn_id),
                    })];
                };
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![(key_space, key), absent],
                    txn_id: Some(txn_id),
                })]
            }
            (State::Read, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                if let Err(error) = check_group_write(value.as_deref()) {
                    return self.fail(error);
                }
                self.write()
            }
            (State::Read, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let mut values = values.into_iter().map(|(_, value)| value);
                if let Err(error) = check_group_write(values.next().flatten().as_deref()) {
                    return self.fail(error);
                }
                if values.next().flatten().is_some() {
                    return self.fail(GroupWriteError::Taken);
                }
                self.write()
            }
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. }))
                if self.secret.is_some() =>
            {
                let Some((entry, secret)) = self.secret.take() else {
                    return self.fail(GroupWriteError::Unexpected);
                };
                self.state = State::Secret;
                smallvec![crate::node_vault::write_secret(entry, secret, self.txn_id)]
            }
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. }))
            | (State::Secret, Event::Storage(StorageEvent::WriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(GroupWriteError::Unexpected);
                };
                self.state = State::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = State::Finish;
                self.output = Some(Ok(()));
                smallvec![]
            }
            _ => self.fail(GroupWriteError::Unexpected),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Finish
    }

    fn finalize(self) -> Result<(), GroupWriteError> {
        self.output.unwrap_or(Err(GroupWriteError::Unexpected))
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map(|txn_id| smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })])
            .unwrap_or_default()
    }
}
