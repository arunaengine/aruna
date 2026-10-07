//! Marks and raises bucket epochs after lost READ scopes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::errors::StorageError;
use aruna_core::events::SubOperationEvent;
use aruna_core::operation::boxed_suboperation;
use aruna_core::types::GroupId;

/// Progress row value: the raised epoch, the phase (grants, then requests) and the last key.
pub(super) fn progress(epoch: u64, requests: bool, cursor: &[u8]) -> Vec<u8> {
    [&epoch.to_be_bytes()[..], &[u8::from(requests)], cursor].concat()
}

pub(super) fn parse_progress(value: &[u8]) -> Option<(u64, bool, Vec<u8>)> {
    let epoch = u64::from_be_bytes(value.get(..8)?.try_into().ok()?);
    match value.get(8)? {
        0 => Some((epoch, false, value[9..].to_vec())),
        1 => Some((epoch, true, value[9..].to_vec())),
        _ => None,
    }
}

impl KeyOperation {
    /// Raises the epoch and restarts reissue in one transaction; a due marker written meanwhile
    /// conflicts with it and stays due.
    pub(super) fn raise(&mut self, instant: bool) -> Effects {
        let Some(snapshot) = &self.snapshot else {
            return self.fail(KeyError::Missing);
        };
        let holder = snapshot.holder && self.auth.path_restrictions.is_none();
        if instant && !holder {
            return self.fail(KeyError::Denied);
        }
        let epoch = snapshot.epoch;
        let bucket: Key = snapshot.parameters.key.bucket_id.to_bytes().to_vec().into();
        if !instant && !(snapshot.due && (holder || self.managed)) {
            self.result = Some(KeyResult::Epoch(epoch));
            return self.flush();
        }
        let Some(next) = epoch.checked_add(1) else {
            return self.fail(AbeError::Limit);
        };
        let row = progress(next, false, &[]);
        self.deletes
            .push((ABE_DUE_KEYSPACE.to_string(), bucket.clone()));
        self.writes.push((
            ABE_EPOCH_KEYSPACE.to_string(),
            bucket.clone(),
            next.to_be_bytes().to_vec().into(),
        ));
        self.writes
            .push((ABE_REISSUE_KEYSPACE.to_string(), bucket, row.into()));
        self.result = Some(KeyResult::Epoch(next));
        self.flush()
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum DueState {
    Scan,
    Read,
    Write,
    Done,
}
/// Marks a raise due in every encrypted bucket of a group, or of the node without a group, inside
/// the transaction of `mark_due`; without one it only lists the node managed buckets to raise.
/// A group without encrypted buckets costs one index scan and nothing else.
#[derive(Debug, PartialEq)]
pub struct EpochDueOperation {
    group_id: Option<GroupId>,
    now: u64,
    txn: Option<TxnId>,
    state: DueState,
    buckets: Vec<String>,
    managed: Vec<String>,
    output: Option<Result<Vec<String>, KeyError>>,
}
impl EpochDueOperation {
    pub fn new(group_id: Option<GroupId>, now: u64) -> Self {
        Self {
            group_id,
            now,
            txn: None,
            state: DueState::Scan,
            buckets: Vec::new(),
            managed: Vec::new(),
            output: None,
        }
    }
    fn done(&mut self) -> Effects {
        self.state = DueState::Done;
        self.output = Some(Ok(std::mem::take(&mut self.managed)));
        smallvec![]
    }
}
impl Operation for EpochDueOperation {
    type Output = Vec<String>;
    type Error = KeyError;
    fn start(&mut self) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: GROUP_ENCRYPTED_KEYSPACE.to_string(),
            prefix: self.group_id.map(|g| g.to_bytes().to_vec().into()),
            start: None,
            limit: usize::MAX,
            txn_id: self.txn
        })]
    }
    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (DueState::Scan, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.buckets = indexed_buckets(&values);
                if self.buckets.is_empty() {
                    return self.done();
                }
                let reads = self
                    .buckets
                    .iter()
                    .map(|b| {
                        (
                            BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                            b.as_bytes().to_vec().into(),
                        )
                    })
                    .collect();
                self.state = DueState::Read;
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads,
                    txn_id: self.txn
                })]
            }
            (DueState::Read, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let (writes, managed) = due_rows(&self.buckets, values, self.now);
                self.managed = managed;
                if writes.is_empty() || self.txn.is_none() {
                    return self.done();
                }
                self.state = DueState::Write;
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes,
                    txn_id: self.txn
                })]
            }
            (DueState::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => self.done(),
            _ => {
                self.state = DueState::Done;
                self.output = Some(Err(KeyError::Storage));
                smallvec![]
            }
        }
    }
    fn is_complete(&self) -> bool {
        self.output.is_some()
    }
    fn finalize(self) -> Result<Vec<String>, KeyError> {
        self.output.unwrap_or(Err(KeyError::Storage))
    }
    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Marks a raise due for a lost READ scope inside the caller's transaction `txn_id`.
pub fn mark_due(group_id: Option<GroupId>, txn_id: TxnId) -> Effect {
    let mut operation = EpochDueOperation::new(group_id, aruna_core::time::unix_timestamp_millis());
    operation.txn = Some(txn_id);
    Effect::SubOperation(boxed_suboperation(operation, |result| {
        let result = result
            .map(|_| ())
            .map_err(|error| StorageError::WriteError(error.to_string()));
        Event::SubOperation(SubOperationEvent::EpochsMarked { result })
    }))
}

/// Commits `txn_id` after `mark_due` succeeded, or returns the marking error.
pub fn marked(event: &Event, txn_id: TxnId) -> Option<Result<Effects, StorageError>> {
    let Event::SubOperation(SubOperationEvent::EpochsMarked { result }) = event else {
        return None;
    };
    let commit = Effect::Storage(StorageEffect::CommitTransaction { txn_id });
    Some(result.clone().map(|()| smallvec![commit]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_early_write() {
        let mut operation = EpochDueOperation::new(None, 1);
        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert!(effects.is_empty());
        assert_eq!(operation.finalize(), Err(KeyError::Storage));
    }
}
