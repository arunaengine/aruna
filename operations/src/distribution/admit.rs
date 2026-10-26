//! Admits one member on its own node in one storage transaction: checks the bucket's binding
//! and the node's capacity, opens and reserves the ranges it holds and signs its admission.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, NetEffect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent, StatementSignEvent, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_CREATION_KEYSPACE, CAPACITY_RESERVATION_KEYSPACE, RANGE_ACCOUNT_KEYSPACE,
    S3_BUCKET_KEYSPACE, USAGE_STATS_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, incarnation};
use aruna_core::structs::storage::distribution::accounting::account_key;
use aruna_core::structs::storage::distribution::capacity::{
    MemberNeed, MemberReservation, RangeSize, member_need,
};
use aruna_core::structs::storage::distribution::{
    DataStrategy, KeyRange, MetadataCoverage, SignedStatement, Statement, StatementClaim,
};
use aruna_core::structs::storage::usage::{
    BucketCounters, USAGE_REBUILD_KEY, UsageCounters, global_shard_keys, usage_bucket_key,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use super::inventory::open_ranges;

/// Reservation rows read per page; a node holds one per member and one per held range.
const RESERVATION_PAGE: usize = 256;

#[derive(Debug, Clone, PartialEq)]
pub struct AdmitConfig {
    pub realm_id: RealmId,
    /// This node; the statement is signed with its key.
    pub node_id: NodeId,
    pub bucket: String,
    pub set_id: Ulid,
    pub member_id: Ulid,
    /// The create or join digest the admission answers.
    pub intent: [u8; 32],
    /// The ranges whose records the member holds; only those not opened yet are written.
    pub ranges: Vec<KeyRange>,
    pub strategy: DataStrategy,
    pub coverage: MetadataCoverage,
    /// Expected sizes of the ranges, from their holders' counters; a missing one is empty.
    pub sizes: Vec<RangeSize>,
    /// The bytes this node may store, the sum of its backend quotas; `None` without quotas.
    pub capacity: Option<u64>,
}

#[derive(Debug, Error, PartialEq)]
pub enum AdmitError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("bucket \"{0}\" is missing or does not carry this member's binding")]
    BindingChanged(String),
    #[error("statement signing unavailable: {0}")]
    SignUnavailable(String),
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: got {got}")]
    UnexpectedEvent { state: &'static str, got: String },
    #[error("usage counters are being rebuilt; new ranges are admitted once they are complete")]
    UsageIncomplete,
    #[error(
        "this node has no storage capacity configured; give its backends quotas before a member \
         with all coverage holds every range here"
    )]
    CapacityUnknown,
    #[error("the member needs about {need} bytes, but this node has {free} bytes of capacity left")]
    InsufficientCapacity { need: u64, free: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmitState {
    Init,
    StartTransaction,
    Read,
    Reservations,
    Usage,
    Write,
    Sign,
    Commit,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct AdmitOperation {
    config: AdmitConfig,
    state: AdmitState,
    txn_id: Option<TxnId>,
    /// The claim to sign once the range rows are staged.
    pending: Option<StatementClaim>,
    /// The ranges to open and what the member needs for them.
    missing: Vec<KeyRange>,
    need: MemberNeed,
    /// Bucket and reserved bytes of every member this node admitted, by set.
    reserved: BTreeMap<Ulid, (Option<String>, u64)>,
    signed: Option<SignedStatement>,
    output: Option<Result<SignedStatement, AdmitError>>,
}

impl AdmitOperation {
    pub fn new(config: AdmitConfig) -> Self {
        Self {
            config,
            state: AdmitState::Init,
            txn_id: None,
            pending: None,
            missing: Vec::new(),
            need: MemberNeed::default(),
            reserved: BTreeMap::new(),
            signed: None,
            output: None,
        }
    }

    fn read(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = AdmitState::Read;
        let bucket: Key = self.config.bucket.as_bytes().into();
        let mut reads = vec![
            (S3_BUCKET_KEYSPACE.to_string(), bucket.clone()),
            (BUCKET_CREATION_KEYSPACE.to_string(), bucket),
        ];
        reads.extend(self.config.ranges.iter().map(|range| {
            let key = account_key(self.config.set_id, range.range_id);
            (RANGE_ACCOUNT_KEYSPACE.to_string(), Key::from(key))
        }));
        // Incomplete usage counters never count as free capacity.
        reads.push((
            USAGE_STATS_KEYSPACE.to_string(),
            Key::from(USAGE_REBUILD_KEY.to_vec()),
        ));
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
    }

    /// The bucket must carry this member's binding for its own incarnation; ranges without an
    /// account row are opened, never while this node's usage counters are being rebuilt.
    fn plan(&mut self, mut values: Vec<Option<Value>>) -> Result<Effects, AdmitError> {
        let rebuilding = values.pop().flatten().is_some();
        let mut values = values.into_iter();
        let bucket = values.next().flatten();
        let creation = values.next().flatten();
        let changed = || AdmitError::BindingChanged(self.config.bucket.clone());
        let info = bucket
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?
            .ok_or_else(changed)?;
        let binding = info.distribution.ok_or_else(changed)?;
        let local = incarnation(creation.as_deref());
        if binding.set_id != self.config.set_id
            || binding.member_id != self.config.member_id
            || binding.incarnation != local
        {
            return Err(changed());
        }
        let missing: Vec<KeyRange> = self
            .config
            .ranges
            .iter()
            .zip(values)
            .filter(|(_, account)| account.is_none())
            .map(|(range, _)| range.clone())
            .collect();
        let statement = Statement::admit(
            self.config.realm_id,
            self.config.set_id,
            self.config.member_id,
            binding.incarnation,
            self.config.intent,
        );
        let claim = StatementClaim {
            signer: self.config.node_id,
            statement,
        };
        if missing.is_empty() {
            return Ok(self.sign(claim));
        }
        if rebuilding {
            return Err(AdmitError::UsageIncomplete);
        }
        let config = &self.config;
        self.need = member_need(config.member_id, config.strategy, &missing, &config.sizes);
        self.missing = missing;
        self.pending = Some(claim);
        match config.capacity {
            Some(_) => self.reservations(None),
            None if config.coverage == MetadataCoverage::All => Err(AdmitError::CapacityUnknown),
            None => self.write(),
        }
    }

    /// Reads one page of the node's reservations inside the transaction, so two admissions
    /// that both fit alone cannot both commit.
    fn reservations(&mut self, start: Option<IterStart>) -> Result<Effects, AdmitError> {
        self.state = AdmitState::Reservations;
        Ok(smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: CAPACITY_RESERVATION_KEYSPACE.to_string(),
            prefix: None,
            start,
            limit: RESERVATION_PAGE,
            txn_id: Some(self.txn_id.ok_or(AdmitError::NotFinished)?),
        })])
    }

    fn add_reservations(
        &mut self,
        values: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Result<Effects, AdmitError> {
        let invalid = || ConversionError::InvalidLength("capacity reservation row".into());
        for (key, value) in values {
            let set_id = key.get(..16).and_then(|set| <[u8; 16]>::try_from(set).ok());
            let set_id = Ulid::from_bytes(set_id.ok_or_else(invalid)?);
            let entry = self.reserved.entry(set_id).or_default();
            let bytes = match key.len() {
                16 => {
                    let member = MemberReservation::from_bytes(value.as_ref())?;
                    entry.0 = Some(member.bucket);
                    member.cache_bytes
                }
                _ => u64::from_be_bytes(value.as_ref().try_into().map_err(|_| invalid())?),
            };
            entry.1 = entry.1.saturating_add(bytes);
        }
        if let Some(next) = next {
            return self.reservations(Some(IterStart::After(next)));
        }
        // Usage rows change with every write; read outside the transaction, they never
        // conflict it.
        let mut reads: Vec<(String, Key)> = global_shard_keys()
            .into_iter()
            .map(|key| (USAGE_STATS_KEYSPACE.to_string(), Key::from(key)))
            .collect();
        reads.extend(self.member_buckets().map(|bucket| {
            let key = Key::from(usage_bucket_key(bucket));
            (USAGE_STATS_KEYSPACE.to_string(), key)
        }));
        self.state = AdmitState::Usage;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })])
    }

    fn member_buckets(&self) -> impl Iterator<Item = &String> {
        self.reserved
            .values()
            .filter_map(|(bucket, _)| bucket.as_ref())
    }

    /// Stored bytes plus the part of each reservation its bucket has not filled yet must leave
    /// room for the member's need.
    fn check_capacity(&mut self, values: Vec<Option<Value>>) -> Result<Effects, AdmitError> {
        let capacity = self.config.capacity.ok_or(AdmitError::NotFinished)?;
        let shards = global_shard_keys().len();
        let mut used = 0u64;
        for value in values.iter().take(shards).flatten() {
            used = used.saturating_add(UsageCounters::from_bytes(value.as_ref())?.stored_bytes);
        }
        let mut filled = values.into_iter().skip(shards);
        for (bucket, reserved) in self.reserved.values() {
            let stored = match bucket {
                Some(_) => filled.next().flatten(),
                None => None,
            };
            let stored = stored
                .map(|value| BucketCounters::from_bytes(value.as_ref()))
                .transpose()?
                .map_or(0, |counters| counters.logical_bytes);
            used = used.saturating_add(reserved.saturating_sub(stored));
        }
        let (need, free) = (self.need.total(), capacity.saturating_sub(used));
        if need > free {
            return Err(AdmitError::InsufficientCapacity { need, free });
        }
        self.write()
    }

    /// Opens the missing ranges and reserves what the member needs for them.
    fn write(&mut self) -> Result<Effects, AdmitError> {
        let txn_id = self.txn_id.ok_or(AdmitError::NotFinished)?;
        let set_id = self.config.set_id;
        let mut writes = open_ranges(set_id, &self.missing)?;
        let member = MemberReservation {
            bucket: self.config.bucket.clone(),
            cache_bytes: self.need.cache_bytes,
        };
        let row = |key: Vec<u8>, value: Vec<u8>| {
            let key_space = CAPACITY_RESERVATION_KEYSPACE.to_string();
            (key_space, Key::from(key), Value::from(value))
        };
        writes.push(row(set_id.to_bytes().to_vec(), member.to_bytes()?));
        for (range_id, bytes) in &self.need.ranges {
            let key = account_key(set_id, *range_id);
            writes.push(row(key, bytes.to_be_bytes().to_vec()));
        }
        self.state = AdmitState::Write;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }

    fn sign(&mut self, claim: StatementClaim) -> Effects {
        self.state = AdmitState::Sign;
        smallvec![Effect::Net(NetEffect::StatementSign(Box::new(claim)))]
    }

    fn commit(&mut self, signed: SignedStatement) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(AdmitError::NotFinished);
        };
        self.signed = Some(signed);
        self.state = AdmitState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn fail(&mut self, error: AdmitError) -> Effects {
        let cleanup = self.abort();
        self.state = AdmitState::Error;
        self.output = Some(Err(error));
        cleanup
    }

    fn unexpected(&mut self, state: &'static str, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        self.fail(AdmitError::UnexpectedEvent {
            state,
            got: format!("{event:?}"),
        })
    }
}

impl Operation for AdmitOperation {
    type Output = SignedStatement;
    type Error = AdmitError;

    fn start(&mut self) -> Effects {
        self.state = AdmitState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (
                AdmitState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => self.read(txn_id),
            (AdmitState::Read, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let values = values.into_iter().map(|(_, value)| value).collect();
                self.plan(values).unwrap_or_else(|error| self.fail(error))
            }
            (
                AdmitState::Reservations,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => (self.add_reservations(values, next_start_after))
                .unwrap_or_else(|error| self.fail(error)),
            (AdmitState::Usage, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                let values = values.into_iter().map(|(_, value)| value).collect();
                self.check_capacity(values)
                    .unwrap_or_else(|error| self.fail(error))
            }
            (AdmitState::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                match self.pending.take() {
                    Some(claim) => self.sign(claim),
                    None => self.fail(AdmitError::NotFinished),
                }
            }
            (AdmitState::Sign, Event::Net(NetEvent::StatementSign(signed))) => match signed {
                StatementSignEvent::Signed(signed) => self.commit(*signed),
                StatementSignEvent::Unavailable(reason) => {
                    self.fail(AdmitError::SignUnavailable(reason))
                }
            },
            (AdmitState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = AdmitState::Finish;
                self.output = self.signed.take().map(Ok);
                smallvec![]
            }
            (AdmitState::Init | AdmitState::Finish | AdmitState::Error, _) => smallvec![],
            (state, event) => {
                let name = match state {
                    AdmitState::StartTransaction => "start transaction",
                    AdmitState::Read => "read",
                    AdmitState::Reservations => "reservations",
                    AdmitState::Usage => "usage",
                    AdmitState::Write => "write",
                    AdmitState::Sign => "sign",
                    AdmitState::Commit => "commit",
                    AdmitState::Init | AdmitState::Finish | AdmitState::Error => "done",
                };
                self.unexpected(name, event)
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, AdmitState::Finish | AdmitState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if !self.is_complete() {
            return Err(AdmitError::NotFinished);
        }
        self.output.unwrap_or(Err(AdmitError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
#[path = "admit_tests.rs"]
mod tests;
