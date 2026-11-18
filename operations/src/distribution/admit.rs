//! Admits one member on its own node in one storage transaction: checks the bucket's binding
//! and the node's capacity, opens and reserves the ranges it holds and signs its admission.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent, StatementSignEvent};
use aruna_core::keyspaces::{
    BUCKET_CREATION_KEYSPACE, CAPACITY_RESERVATION_KEYSPACE, RANGE_ACCOUNT_KEYSPACE,
    S3_BUCKET_KEYSPACE, USAGE_STATS_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::done;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BucketInfo, incarnation};
use aruna_core::structs::storage::distribution::accounting::account_key;
use aruna_core::structs::storage::distribution::capacity::{
    MemberNeed, MemberReservation, RangeSize, member_need,
};
use aruna_core::structs::storage::distribution::snapshot::RangeFill;
use aruna_core::structs::storage::distribution::{
    DataStrategy, KeyRange, MetadataCoverage, SignedStatement, Statement, StatementClaim,
};
use aruna_core::structs::storage::usage::{
    BucketCounters, USAGE_REBUILD_KEY, UsageCounters, global_shard_keys, usage_bucket_key,
};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

use super::inventory::{StagedCall, open_ranges, outside_answer};

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
    /// Expected sizes of the ranges, from their holders' counters; a missing one is unknown.
    pub sizes: Vec<RangeSize>,
    /// The bytes this node may store, the sum of its backend quotas; `None` without quotas.
    pub capacity: Option<u64>,
    /// The member's new copies not admitted yet; the admission then states them instead.
    pub fills: Vec<RangeFill>,
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
    #[error("unexpected answer to the signing request: {0}")]
    UnexpectedEvent(&'static str),
    #[error("usage counters are being rebuilt; new ranges are admitted once they are complete")]
    UsageIncomplete,
    #[error(
        "this node has no storage capacity configured; give its backends quotas before a member \
         with all coverage holds every range here"
    )]
    CapacityUnknown,
    #[error("the member needs about {need} bytes, but this node has {free} bytes of capacity left")]
    InsufficientCapacity { need: u64, free: u64 },
    #[error(
        "the size of range {0} is not known yet: its holders did not answer, were not asked or \
         are rebuilding their counters; retry the admission later"
    )]
    SizeUnknown(Ulid),
}

impl From<IoError> for AdmitError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// What the pending call of an admission answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmitState {
    Read,
    Reservations,
    Usage,
    Write,
    Sign,
}

type AdmitNext = Result<Next<SignedStatement>, AdmitError>;

#[derive(Debug, PartialEq)]
pub struct AdmitOperation {
    config: AdmitConfig,
    state: AdmitState,
    /// The claim to sign once the range rows are staged.
    pending: Option<StatementClaim>,
    /// The ranges to open and what the member needs for them.
    missing: Vec<KeyRange>,
    need: MemberNeed,
    /// Open ranges the member now copies, and the bytes already reserved for them.
    grown: Vec<Ulid>,
    held: u64,
    /// Bucket and reserved bytes of every member this node admitted, by set.
    reserved: BTreeMap<Ulid, (Option<String>, u64)>,
}

impl AdmitOperation {
    pub fn new(config: AdmitConfig) -> Self {
        Self {
            config,
            state: AdmitState::Read,
            pending: None,
            missing: Vec::new(),
            need: MemberNeed::default(),
            grown: Vec::new(),
            held: 0,
            reserved: BTreeMap::new(),
        }
    }

    fn call(&mut self, state: AdmitState, call: IoCall) -> AdmitNext {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn read(&self) -> IoCall {
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
        IoCall::Read(reads)
    }

    /// The bucket must carry this member's binding for its own incarnation; ranges without an
    /// account row are opened, never while this node's usage counters are being rebuilt. An open
    /// range the member gains a copy of reserves its bytes too.
    fn plan(&mut self, mut values: Vec<Option<Value>>) -> AdmitNext {
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
        let config = &self.config;
        let copied = |range: &KeyRange| (config.fills.iter()).any(|f| f.range_id == range.range_id);
        let (mut missing, mut grown) = (Vec::new(), Vec::new());
        for (range, account) in config.ranges.iter().zip(values) {
            match account {
                None => missing.push(range.clone()),
                Some(_) if copied(range) => grown.push(range.clone()),
                Some(_) => {}
            }
        }
        let statement = match config.fills.is_empty() {
            true => Statement::admit(
                config.realm_id,
                config.set_id,
                config.member_id,
                binding.incarnation,
                config.intent,
            ),
            false => Statement::copy_admitted(
                config.realm_id,
                config.set_id,
                (config.member_id, binding.incarnation, config.intent),
                config.fills.iter().map(|fill| fill.fill_id).collect(),
            ),
        };
        let claim = StatementClaim {
            signer: self.config.node_id,
            statement,
        };
        if missing.is_empty() && grown.is_empty() {
            return self.sign(claim);
        }
        if rebuilding {
            return Err(AdmitError::UsageIncomplete);
        }
        let config = &self.config;
        let needed = [missing.as_slice(), grown.as_slice()].concat();
        self.need = member_need(config.member_id, config.strategy, &needed, &config.sizes)
            .map_err(AdmitError::SizeUnknown)?;
        self.grown = grown.iter().map(|range| range.range_id).collect();
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
    fn reservations(&mut self, after: Option<Key>) -> AdmitNext {
        let page = IoCall::Page {
            space: CAPACITY_RESERVATION_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: RESERVATION_PAGE,
        };
        self.call(AdmitState::Reservations, page)
    }

    fn add_reservations(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> AdmitNext {
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
            let config = &self.config;
            let grown =
                (self.grown.iter()).any(|id| key.as_ref() == account_key(config.set_id, *id));
            if grown {
                self.held = self.held.saturating_add(bytes);
            }
            entry.1 = entry.1.saturating_add(bytes);
        }
        if next.is_some() {
            return self.reservations(next);
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
        let usage = StagedCall::Outside(IoCall::Read(reads)).into_call();
        self.call(AdmitState::Usage, usage)
    }

    fn member_buckets(&self) -> impl Iterator<Item = &String> {
        self.reserved
            .values()
            .filter_map(|(bucket, _)| bucket.as_ref())
    }

    /// Stored bytes plus the part of each reservation its bucket has not filled yet must leave
    /// room for the member's need.
    fn check_capacity(&mut self, values: Vec<Option<Value>>) -> AdmitNext {
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
        let need = self.need.total().saturating_sub(self.held);
        let free = capacity.saturating_sub(used);
        if need > free {
            return Err(AdmitError::InsufficientCapacity { need, free });
        }
        self.write()
    }

    /// Opens the missing ranges and reserves what the member needs for them and its new copies.
    fn write(&mut self) -> AdmitNext {
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
        if !self.missing.is_empty() {
            writes.push(row(set_id.to_bytes().to_vec(), member.to_bytes()?));
        }
        for (range_id, bytes) in &self.need.ranges {
            let key = account_key(set_id, *range_id);
            writes.push(row(key, bytes.to_be_bytes().to_vec()));
        }
        self.call(AdmitState::Write, IoCall::Write(writes))
    }

    fn sign(&mut self, claim: StatementClaim) -> AdmitNext {
        let sign = Effect::Net(NetEffect::StatementSign(Box::new(claim)));
        self.call(AdmitState::Sign, IoCall::Effect(Box::new(sign)))
    }
}

impl Operation for AdmitOperation {
    type Output = SignedStatement;
    type Error = AdmitError;

    fn first(&mut self) -> AdmitNext {
        Ok(Next::Call(self.read()))
    }

    fn next(&mut self, result: IoResult) -> AdmitNext {
        match self.state {
            AdmitState::Read => self.plan(result.values()?),
            AdmitState::Reservations => {
                let (rows, next) = result.page()?;
                self.add_reservations(rows, next)
            }
            AdmitState::Usage => self.check_capacity(outside_answer(result)?.values()?),
            AdmitState::Write => {
                done(result)?;
                let claim = self.pending.take().ok_or(AdmitError::NotFinished)?;
                self.sign(claim)
            }
            AdmitState::Sign => match result.event()? {
                Event::Net(NetEvent::StatementSign(StatementSignEvent::Signed(signed))) => {
                    Ok(Next::Done(*signed))
                }
                Event::Net(NetEvent::StatementSign(StatementSignEvent::Unavailable(reason))) => {
                    Err(AdmitError::SignUnavailable(reason))
                }
                other => Err(AdmitError::UnexpectedEvent(other.label())),
            },
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
#[path = "admit_tests.rs"]
mod tests;
