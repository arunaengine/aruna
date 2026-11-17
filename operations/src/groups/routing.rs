//! Gathers the routing inputs of one write: bucket rules, the group's inputs and which capped
//! backends are full. A failed read fails the write instead of routing it to the node default.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{S3_BUCKET_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::{BackendRef, BucketInfo};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::routing::{
    BackendCatalog, NodeRouting, RoutingSnapshot, StorageRoutingRule,
};
use aruna_core::structs::storage::usage::{UsageCounters, usage_backend_keys};
use aruna_core::types::{Effects, GroupId, Key, Value};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::groups::backends::{RecordReadError, parse_read};
use crate::groups::storage_routing::{GroupInputsError, GroupInputsOperation};

/// Why a write could not learn where it belongs. Absent records are not an
/// error; only an unreadable or undecodable one is.
#[derive(Debug, Error, PartialEq)]
pub enum RoutingInputsError {
    #[error("group routing inputs unavailable: {0}")]
    GroupInputs(#[from] GroupInputsError),
    #[error("bucket routing rules unavailable: {0}")]
    BucketRules(#[from] RecordReadError),
    /// Not `#[from]`: `BucketRules` already owns the conversion from a read.
    #[error("backend usage counters unavailable: {0}")]
    BackendUsage(#[source] RecordReadError),
    #[error("routing inputs never completed")]
    Incomplete,
}

impl RoutingInputsError {
    /// The underlying storage failure, so retrying callers can tell a transient
    /// read failure from a record that will never decode.
    pub fn storage(&self) -> Option<&StorageError> {
        let read = match self {
            Self::GroupInputs(GroupInputsError::Read(read)) => read,
            Self::BucketRules(read) | Self::BackendUsage(read) => read,
            Self::GroupInputs(GroupInputsError::Incomplete | GroupInputsError::TooManyRules(_))
            | Self::Incomplete => return None,
        };
        match read {
            RecordReadError::Storage(error) => Some(error),
            RecordReadError::Conversion(_) | RecordReadError::Unexpected => None,
        }
    }
}

/// Where the bucket's own rules come from.
#[derive(Debug, PartialEq)]
pub enum BucketRules {
    /// Read from the bucket record; a bucket without a record has no rules and no compression.
    Named(String),
    /// The caller already holds the bucket record, as the S3 surface does from its middleware.
    Known {
        rules: Vec<StorageRoutingRule>,
        compression: Compression,
    },
}

impl BucketRules {
    pub fn of(bucket: &BucketInfo) -> Self {
        Self::Known {
            rules: bucket.storage_routing.clone(),
            compression: bucket.compression,
        }
    }
}

#[derive(Debug, PartialEq)]
enum SnapshotState {
    Init(BucketRules),
    Bucket,
    Group(GroupInputsOperation),
    Usage(Vec<(String, u64)>),
    Done,
}

/// Routing inputs for one bucket write: node routing, bucket rules and compression, the owning
/// group's inputs, then the fullness of each capped backend frozen for this request.
#[derive(Debug, PartialEq)]
pub struct SnapshotOperation {
    snapshot: Option<RoutingSnapshot>,
    state: SnapshotState,
    output: Option<Result<RoutingSnapshot, RoutingInputsError>>,
}

impl SnapshotOperation {
    pub fn new(routing: &NodeRouting, group_id: GroupId, bucket: BucketRules) -> Self {
        Self {
            snapshot: Some(routing.snapshot(group_id)),
            state: SnapshotState::Init(bucket),
            output: None,
        }
    }

    fn rules(&mut self, rules: Vec<StorageRoutingRule>, compression: Compression) -> Effects {
        let Some(snapshot) = self.snapshot.take() else {
            return self.finish(Err(RoutingInputsError::Incomplete));
        };
        let mut inputs = GroupInputsOperation::new(snapshot.group_id);
        self.snapshot = Some(snapshot.with_bucket_rules(rules).with_compression(compression));
        let effects = inputs.start();
        self.state = SnapshotState::Group(inputs);
        effects
    }

    fn group(&mut self, mut inputs: GroupInputsOperation, event: Event) -> Effects {
        let effects = inputs.step(event);
        if !inputs.is_complete() {
            self.state = SnapshotState::Group(inputs);
            return effects;
        }
        let (inputs, Some(snapshot)) = (inputs.finalize(), self.snapshot.take()) else {
            return self.finish(Err(RoutingInputsError::Incomplete));
        };
        let snapshot = match inputs {
            Ok(inputs) => snapshot.with_group_inputs(inputs),
            Err(error) => return self.finish(Err(error.into())),
        };
        let quotas = snapshot.catalog.quotas();
        self.snapshot = Some(snapshot);
        if quotas.is_empty() {
            return self.finish(Ok(()));
        }
        let effect = usage_read(&quotas);
        self.state = SnapshotState::Usage(quotas);
        smallvec![effect]
    }

    fn usage(&mut self, quotas: &[(String, u64)], event: Event) -> Effects {
        let Some(mut snapshot) = self.snapshot.take() else {
            return self.finish(Err(RoutingInputsError::Incomplete));
        };
        match full_catalog(snapshot.catalog.clone(), quotas, event) {
            Ok(catalog) => {
                snapshot.catalog = catalog;
                self.snapshot = Some(snapshot);
                self.finish(Ok(()))
            }
            Err(error) => self.finish(Err(RoutingInputsError::BackendUsage(error))),
        }
    }

    fn finish(&mut self, result: Result<(), RoutingInputsError>) -> Effects {
        self.state = SnapshotState::Done;
        self.output = Some(result.and_then(|()| {
            self.snapshot
                .take()
                .ok_or(RoutingInputsError::Incomplete)
        }));
        smallvec![]
    }
}

impl Operation for SnapshotOperation {
    type Output = RoutingSnapshot;
    type Error = RoutingInputsError;

    fn start(&mut self) -> Effects {
        match std::mem::replace(&mut self.state, SnapshotState::Bucket) {
            SnapshotState::Init(BucketRules::Named(bucket)) => smallvec![Effect::Storage(
                StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: bucket.into_bytes().into(),
                    txn_id: None,
                }
            )],
            SnapshotState::Init(BucketRules::Known { rules, compression }) => {
                self.rules(rules, compression)
            }
            state => {
                self.state = state;
                smallvec![]
            }
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match std::mem::replace(&mut self.state, SnapshotState::Done) {
            SnapshotState::Bucket => match parse_read(event, BucketInfo::from_bytes) {
                Ok(info) => {
                    let (rules, compression) = info
                        .map(|info| (info.storage_routing, info.compression))
                        .unwrap_or_default();
                    self.rules(rules, compression)
                }
                Err(error) => self.finish(Err(error.into())),
            },
            SnapshotState::Group(inputs) => self.group(inputs, event),
            SnapshotState::Usage(quotas) => self.usage(&quotas, event),
            SnapshotState::Init(_) | SnapshotState::Done => {
                self.finish(Err(RoutingInputsError::Incomplete))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<RoutingSnapshot, RoutingInputsError> {
        self.output.unwrap_or(Err(RoutingInputsError::Incomplete))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Node routing whose capped backends already carry their fullness, for the background writers
/// that build their own snapshot later. Replication reads the same catalog, so an unreadable
/// counter refuses it too and it retries.
#[derive(Debug, PartialEq)]
pub struct QuotaOperation {
    routing: Option<NodeRouting>,
    quotas: Vec<(String, u64)>,
    output: Option<Result<NodeRouting, RoutingInputsError>>,
}

impl QuotaOperation {
    pub fn new(routing: NodeRouting) -> Self {
        Self {
            quotas: routing.catalog.quotas(),
            routing: Some(routing),
            output: None,
        }
    }
}

impl Operation for QuotaOperation {
    type Output = NodeRouting;
    type Error = RoutingInputsError;

    fn start(&mut self) -> Effects {
        if self.quotas.is_empty() {
            self.output = self.routing.take().map(Ok);
            return smallvec![];
        }
        smallvec![usage_read(&self.quotas)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let Some(routing) = self.routing.take() else {
            self.output = Some(Err(RoutingInputsError::Incomplete));
            return smallvec![];
        };
        self.output = Some(match full_catalog(routing.catalog.clone(), &self.quotas, event) {
            Ok(catalog) => Ok(NodeRouting { catalog, ..routing }),
            Err(error) => Err(RoutingInputsError::BackendUsage(error)),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<NodeRouting, RoutingInputsError> {
        self.output.unwrap_or(Err(RoutingInputsError::Incomplete))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Stored bytes of one backend, summed over its counter shards. A missing row reads as zero, so a
/// node whose counters were never built reports no usage; an unreadable shard is an error.
#[derive(Debug, PartialEq)]
pub struct UsageOperation {
    backend: BackendRef,
    started: bool,
    output: Option<Result<u64, RecordReadError>>,
}

impl UsageOperation {
    pub fn new(backend: BackendRef) -> Self {
        Self {
            backend,
            started: false,
            output: None,
        }
    }
}

impl Operation for UsageOperation {
    type Output = u64;
    type Error = RecordReadError;

    fn start(&mut self) -> Effects {
        self.started = true;
        smallvec![batch_read(usage_backend_keys(&self.backend))]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.output = Some(if self.started && self.output.is_none() {
            batch_values(event).and_then(|values| used_bytes(&values))
        } else {
            Err(RecordReadError::Unexpected)
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<u64, RecordReadError> {
        self.output.unwrap_or(Err(RecordReadError::Unexpected))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// One batch read of every counter shard of the capped backends, backend by backend.
fn usage_read(quotas: &[(String, u64)]) -> Effect {
    batch_read(
        quotas
            .iter()
            .flat_map(|(name, _)| usage_backend_keys(&BackendRef::Node(name.clone())))
            .collect(),
    )
}

fn batch_read(keys: Vec<Vec<u8>>) -> Effect {
    let reads = keys
        .into_iter()
        .map(|key| (USAGE_STATS_KEYSPACE.to_string(), key.into()))
        .collect();
    Effect::Storage(StorageEffect::BatchRead {
        reads,
        txn_id: None,
    })
}

fn batch_values(event: Event) -> Result<Vec<(Key, Option<Value>)>, RecordReadError> {
    match event {
        Event::Storage(StorageEvent::BatchReadResult { values }) => Ok(values),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(RecordReadError::Unexpected),
    }
}

fn used_bytes(values: &[(Key, Option<Value>)]) -> Result<u64, RecordReadError> {
    let mut total = 0u64;
    for value in values.iter().filter_map(|(_, value)| value.as_ref()) {
        total = total.saturating_add(UsageCounters::from_bytes(value.as_ref())?.stored_bytes);
    }
    Ok(total)
}

/// Freezes each capped backend's fullness for one request, exactly like the group quota ceiling.
/// Concurrent writes can overshoot by their own bytes; an unreadable counter fails the caller.
fn full_catalog(
    mut catalog: BackendCatalog,
    quotas: &[(String, u64)],
    event: Event,
) -> Result<BackendCatalog, RecordReadError> {
    let values = batch_values(event)?;
    let mut rest = values.as_slice();
    for (name, quota) in quotas {
        let shards = usage_backend_keys(&BackendRef::Node(name.clone())).len();
        if rest.len() < shards {
            return Err(RecordReadError::Unexpected);
        }
        let (own, tail) = rest.split_at(shards);
        rest = tail;
        if used_bytes(own)? >= *quota {
            warn!(backend = %name, quota_bytes = quota, "Storage backend reached its quota");
            catalog = catalog.mark_full(name);
        }
    }
    if !rest.is_empty() {
        return Err(RecordReadError::Unexpected);
    }
    Ok(catalog)
}

#[cfg(test)]
#[path = "routing_tests.rs"]
mod tests;
