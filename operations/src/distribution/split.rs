//! Splits the accounting of one range: freezes the parent's counters as the charged snapshot,
//! opens two child ranges with pending rows and rebuilds, divides sample and reservation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    CAPACITY_RESERVATION_KEYSPACE, KEY_SAMPLE_KEYSPACE, RANGE_ACCOUNT_KEYSPACE,
    RANGE_COUNTER_KEYSPACE, RANGE_INDEX_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::distribution::accounting::{
    Charge, RangeAccount, RangeCounters, RangeSlot, Rebuild, SAMPLE_SIZE, SampleEntry, account_key,
    generation_key, pending_key, sample_count_key, sample_prefix, slot_key,
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
    Count,
    Commit,
    Done,
}

#[derive(Debug, PartialEq)]
pub struct SplitOperation {
    config: SplitConfig,
    state: State,
    txn_id: Option<TxnId>,
    parent: Option<RangeAccount>,
    /// The bytes this node reserved for the parent at admission.
    reserved: Option<u64>,
    deletes: Vec<(String, Key)>,
    counts: Vec<(Key, RangeCounters)>,
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
            reserved: None,
            deletes: Vec::new(),
            counts: Vec::new(),
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
                frozen: generation,
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
        let (mut left_size, mut right_size) = (0, 0);
        for (key, _) in sample {
            let Some(entry) = SampleEntry::decode(&key) else {
                continue;
            };
            let owner = match entry.key < split {
                true => (config.left, &mut left_size),
                false => (config.right, &mut right_size),
            };
            *owner.1 += 1;
            writes.push(row(
                KEY_SAMPLE_KEYSPACE,
                entry.row_key(set_id, owner.0),
                vec![],
            ));
            self.deletes.push((KEY_SAMPLE_KEYSPACE.to_string(), key));
        }
        if let Some(bytes) = self.reserved {
            // The children take over the parent's reservation, divided like the sample.
            let left_bytes = match left_size + right_size {
                0 => bytes / 2,
                sampled => u64::try_from(i128::from(bytes) * left_size / sampled).unwrap_or(bytes),
            };
            for (child, part) in [
                (config.left, left_bytes),
                (config.right, bytes - left_bytes),
            ] {
                let key = account(child);
                writes.push(row(
                    CAPACITY_RESERVATION_KEYSPACE,
                    key,
                    part.to_be_bytes().into(),
                ));
            }
            let parent_row = account(parent_id).into();
            self.deletes
                .push((CAPACITY_RESERVATION_KEYSPACE.to_string(), parent_row));
        }
        let parent_count = sample_count_key(set_id, parent_id);
        self.deletes
            .push((RANGE_COUNTER_KEYSPACE.to_string(), parent_count.into()));
        for (child, size) in [(config.left, left_size), (config.right, right_size)] {
            let counters = RangeCounters {
                sampled: size,
                ..RangeCounters::default()
            };
            self.counts
                .push((sample_count_key(set_id, child).into(), counters));
        }
        Ok(writes)
    }

    fn handle(&mut self, event: Event) -> Result<Effects, SplitError> {
        let (set_id, range_id) = (self.config.set_id, self.config.range_id);
        match (&self.state, event) {
            (State::Started, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = State::ReadAccount;
                let key = Key::from(account_key(set_id, range_id));
                Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![
                        (RANGE_ACCOUNT_KEYSPACE.to_string(), key.clone()),
                        (CAPACITY_RESERVATION_KEYSPACE.to_string(), key),
                    ],
                    txn_id: Some(txn_id),
                })])
            }
            (State::ReadAccount, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let mut values = values.into_iter().map(|(_, value)| value);
                let value = values.next().flatten();
                let value = value.ok_or(SplitError::RangeMissing(range_id))?;
                let parent = RangeAccount::from_bytes(&value)?;
                self.check(&parent)?;
                self.parent = Some(parent);
                let invalid = || ConversionError::InvalidLength("capacity reservation row".into());
                let reserved = values.next().flatten().map(|value| {
                    let bytes = value.as_ref().try_into().map_err(|_| invalid());
                    bytes.map(u64::from_be_bytes)
                });
                self.reserved = reserved.transpose()?;
                self.state = State::ReadSample;
                Ok(smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: KEY_SAMPLE_KEYSPACE.to_string(),
                    prefix: Some(sample_prefix(set_id, range_id).into()),
                    start: None,
                    limit: usize::from(SAMPLE_SIZE) * 2,
                    txn_id: self.txn_id,
                })])
            }
            (
                State::ReadSample,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                // Entries beyond the read would stay behind under the retired parent.
                if next_start_after.is_some() {
                    return Err(SplitError::Refused(
                        range_id,
                        "the key sample needs a refill",
                    ));
                }
                let writes = self.plan(values)?;
                self.state = State::Write;
                Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes,
                    txn_id: self.txn_id,
                })])
            }
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.state = State::Delete;
                Ok(smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes: std::mem::take(&mut self.deletes),
                    txn_id: self.txn_id,
                })])
            }
            (State::Delete, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                self.state = State::Count;
                Ok(smallvec![Effect::Storage(StorageEffect::AddCounters {
                    key_space: RANGE_COUNTER_KEYSPACE.to_string(),
                    deltas: std::mem::take(&mut self.counts),
                    txn_id: self.txn_id.unwrap_or_default(),
                })])
            }
            (State::Count, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
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

#[cfg(test)]
#[path = "split_tests.rs"]
mod tests;
