//! Commits group resource records only while the group's deletion fence is open.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::GROUP_DELETE_KEYSPACE;
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
    Commit,
    Finish,
}

#[derive(Debug, PartialEq)]
pub struct GroupWriteOperation {
    group_id: GroupId,
    writes: Vec<(String, Key, Value)>,
    txn_id: Option<TxnId>,
    state: State,
    output: Option<Result<(), GroupWriteError>>,
}

impl GroupWriteOperation {
    pub fn new(group_id: GroupId, writes: Vec<(String, Key, Value)>) -> Self {
        Self {
            group_id,
            writes,
            txn_id: None,
            state: State::Init,
            output: None,
        }
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
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space,
                    key,
                    txn_id: Some(txn_id),
                })]
            }
            (State::Read, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                if let Err(error) = check_group_write(value.as_deref()) {
                    return self.fail(error);
                }
                self.state = State::Write;
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes: std::mem::take(&mut self.writes),
                    txn_id: self.txn_id,
                })]
            }
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
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
