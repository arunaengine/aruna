//! Splits the accounting of one range: freezes the parent's counters as the charged
//! snapshot, opens two child ranges with pending rows and rebuilds, and divides the sample.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{KEY_SAMPLE_KEYSPACE, RANGE_ACCOUNT_KEYSPACE, RANGE_INDEX_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::distribution::accounting::{
    Charge, RangeAccount, RangeSlot, Rebuild, SAMPLE_SIZE, SampleSlot, account_key, generation_key,
    pending_key, sample_key, sample_prefix, slot_key,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error, PartialEq)]
pub enum SplitError {
    #[error("range {0} has no account row")]
    RangeMissing(Ulid),
    #[error("range {0} cannot split now: {1}")]
    Refused(Ulid, &'static str),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("split received unexpected event: {0:?}")]
    UnexpectedEvent(Box<Event>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SplitConfig {
    pub set_id: Ulid,
    pub range_id: Ulid,
    /// The first key of the right child.
    pub split_key: String,
    pub left: Ulid,
    pub right: Ulid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum State {
    Init,
    Started,
    ReadAccount,
    ReadSample,
    Write,
    Delete,
    Commit,
    Done,
}

#[derive(Debug, PartialEq)]
pub struct SplitOperation {
    config: SplitConfig,
    state: State,
    txn_id: Option<TxnId>,
    parent: Option<RangeAccount>,
    deletes: Vec<(String, Key)>,
    result: Option<Result<(), SplitError>>,
}

/// The counter rows that make up a range's charged total: its own row, or for a split
/// parent its frozen row plus both children's pending rows. A child is charged through its
/// parent until the handoff.
pub fn charged_rows(set_id: Ulid, range_id: Ulid, account: &RangeAccount) -> Vec<Vec<u8>> {
    match account.charge {
        Charge::Active(generation) => vec![generation_key(set_id, range_id, generation)],
        Charge::Retired {
            generation,
            left,
            right,
        } => vec![
            generation_key(set_id, range_id, generation),
            pending_key(set_id, left),
            pending_key(set_id, right),
        ],
        Charge::Child { .. } => Vec::new(),
    }
}

fn row(key_space: &str, key: Vec<u8>, value: Vec<u8>) -> (String, Key, Value) {
    (key_space.to_string(), key.into(), value.into())
}

impl SplitOperation {
    pub fn new(config: SplitConfig) -> Self {
        Self {
            config,
            state: State::Init,
            txn_id: None,
            parent: None,
            deletes: Vec::new(),
            result: None,
        }
    }

    fn finish(&mut self, result: Result<(), SplitError>) -> Effects {
        self.state = State::Done;
        self.result = Some(result);
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }

    fn check(&self, parent: &RangeAccount) -> Result<u64, SplitError> {
        let refused = |reason| SplitError::Refused(self.config.range_id, reason);
        let Charge::Active(generation) = parent.charge else {
            return Err(refused("its counters are not active"));
        };
        if parent.rebuild.is_some() {
            return Err(refused("its counters are being rebuilt"));
        }
        let key = self.config.split_key.as_str();
        if key <= parent.lower.as_str() || !parent.contains(key) {
            return Err(refused("the split key leaves a child empty"));
        }
        Ok(generation)
    }

    /// The parent, child and index rows of the split, with the sample moved to the children.
    fn plan(&mut self, sample: Vec<(Key, Value)>) -> Result<Vec<(String, Key, Value)>, SplitError> {
        let config = &self.config;
        let (set_id, parent_id) = (config.set_id, config.range_id);
        let parent = self
            .parent
            .clone()
            .ok_or(SplitError::RangeMissing(parent_id))?;
        let generation = self.check(&parent)?;
        let child = |lower: String, upper: Option<String>, sibling| RangeAccount {
            lower,
            upper,
            charge: Charge::Child {
                parent: parent_id,
                sibling,
            },
            rebuild: Some(Rebuild::new(1)),
        };
        let split = config.split_key.clone();
        let left = child(parent.lower.clone(), Some(split.clone()), config.right);
        let right = child(split.clone(), parent.upper.clone(), config.left);
        let retired = RangeAccount {
            charge: Charge::Retired {
                generation,
                left: config.left,
                right: config.right,
            },
            ..parent.clone()
        };
        let left_slot = RangeSlot {
            range_id: config.left,
            lower: parent.lower.clone(),
        };
        let right_slot = RangeSlot {
            range_id: config.right,
            lower: split.clone(),
        };
        let account = |range_id| account_key(set_id, range_id);
        let mut writes = vec![
            row(
                RANGE_ACCOUNT_KEYSPACE,
                account(parent_id),
                retired.to_bytes()?,
            ),
            row(
                RANGE_ACCOUNT_KEYSPACE,
                account(config.left),
                left.to_bytes()?,
            ),
            row(
                RANGE_ACCOUNT_KEYSPACE,
                account(config.right),
                right.to_bytes()?,
            ),
            row(
                RANGE_INDEX_KEYSPACE,
                slot_key(set_id, Some(&split)),
                left_slot.to_bytes()?,
            ),
            row(
                RANGE_INDEX_KEYSPACE,
                slot_key(set_id, parent.upper.as_deref()),
                right_slot.to_bytes()?,
            ),
        ];
        for (key, value) in sample {
            let entry = SampleSlot::from_bytes(&value)?;
            let (slot, _) = SampleSlot::of_key(&entry.key);
            let owner = match entry.key < split {
                true => config.left,
                false => config.right,
            };
            writes.push(row(
                KEY_SAMPLE_KEYSPACE,
                sample_key(set_id, owner, slot),
                value.to_vec(),
            ));
            self.deletes.push((KEY_SAMPLE_KEYSPACE.to_string(), key));
        }
        Ok(writes)
    }

    fn handle(&mut self, event: Event) -> Result<Effects, SplitError> {
        let (set_id, range_id) = (self.config.set_id, self.config.range_id);
        match (&self.state, event) {
            (State::Started, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = State::ReadAccount;
                Ok(smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: RANGE_ACCOUNT_KEYSPACE.to_string(),
                    key: account_key(set_id, range_id).into(),
                    txn_id: Some(txn_id),
                })])
            }
            (State::ReadAccount, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let value = value.ok_or(SplitError::RangeMissing(range_id))?;
                let parent = RangeAccount::from_bytes(&value)?;
                self.check(&parent)?;
                self.parent = Some(parent);
                self.state = State::ReadSample;
                Ok(smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: KEY_SAMPLE_KEYSPACE.to_string(),
                    prefix: Some(sample_prefix(set_id, range_id).into()),
                    start: None,
                    limit: usize::from(SAMPLE_SIZE),
                    txn_id: self.txn_id,
                })])
            }
            (State::ReadSample, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                let writes = self.plan(values)?;
                self.state = State::Write;
                Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes,
                    txn_id: self.txn_id,
                })])
            }
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                if self.deletes.is_empty() {
                    return Ok(self.commit());
                }
                self.state = State::Delete;
                Ok(smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes: std::mem::take(&mut self.deletes),
                    txn_id: self.txn_id,
                })])
            }
            (State::Delete, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                Ok(self.commit())
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                Ok(self.finish(Ok(())))
            }
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (_, event) => Err(SplitError::UnexpectedEvent(Box::new(event))),
        }
    }

    fn commit(&mut self) -> Effects {
        self.state = State::Commit;
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })],
            None => self.finish(Err(SplitError::Refused(
                self.config.range_id,
                "no transaction",
            ))),
        }
    }
}

impl Operation for SplitOperation {
    type Output = ();
    type Error = SplitError;

    fn start(&mut self) -> Effects {
        self.state = State::Started;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.state == State::Done {
            return smallvec![];
        }
        match self.handle(event) {
            Ok(effects) => effects,
            Err(error) => {
                if matches!(self.state, State::Commit) {
                    self.txn_id = None;
                }
                self.finish(Err(error))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Done
    }

    fn finalize(self) -> Result<(), SplitError> {
        let range_id = self.config.range_id;
        self.result.unwrap_or(Err(SplitError::Refused(
            range_id,
            "the split stopped early",
        )))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

