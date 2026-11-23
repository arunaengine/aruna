//! Gathers the routing inputs of one write: bucket rules, the group's inputs and which capped
//! backends are full. A failed read fails the write instead of routing it to the node default.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::{S3_BUCKET_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::storage::blob::{BackendRef, BucketInfo};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::routing::{
    BackendCatalog, GroupRoutingInputs, NodeRouting, RoutingSnapshot, StorageRoutingRule,
};
use aruna_core::structs::storage::usage::{UsageCounters, usage_backend_keys};
use aruna_core::types::{GroupId, Value};
use thiserror::Error;
use tracing::warn;

use crate::groups::backends::RecordReadError;
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

impl From<IoError> for RoutingInputsError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::BucketRules(RecordReadError::Storage(error)),
            _ => Self::Incomplete,
        }
    }
}

/// What the snapshot waits for.
#[derive(Debug, PartialEq)]
enum SnapshotState {
    Init(BucketRules),
    Bucket,
    Group,
    Usage(Vec<(String, u64)>),
    Done,
}

type SnapshotTurn = Result<Next<RoutingSnapshot>, RoutingInputsError>;

/// Routing inputs for one bucket write: node routing, bucket rules and compression, the owning
/// group's inputs, then the fullness of each capped backend frozen for this request.
#[derive(Debug, PartialEq)]
pub struct SnapshotOperation {
    snapshot: Option<RoutingSnapshot>,
    state: SnapshotState,
}

impl SnapshotOperation {
    pub fn new(routing: &NodeRouting, group_id: GroupId, bucket: BucketRules) -> Self {
        Self {
            snapshot: Some(routing.snapshot(group_id)),
            state: SnapshotState::Init(bucket),
        }
    }

    fn rules(&mut self, rules: Vec<StorageRoutingRule>, compression: Compression) -> SnapshotTurn {
        let snapshot = self.snapshot.take().ok_or(RoutingInputsError::Incomplete)?;
        let inputs = GroupInputsOperation::new(snapshot.group_id);
        self.snapshot = Some(
            snapshot
                .with_bucket_rules(rules)
                .with_compression(compression),
        );
        self.state = SnapshotState::Group;
        Ok(Next::Call(IoCall::child(inputs)))
    }

    fn group(&mut self, inputs: Result<GroupRoutingInputs, GroupInputsError>) -> SnapshotTurn {
        let snapshot = self.snapshot.take().ok_or(RoutingInputsError::Incomplete)?;
        let snapshot = snapshot.with_group_inputs(inputs?);
        let quotas = snapshot.catalog.quotas();
        if quotas.is_empty() {
            return Ok(Next::Done(snapshot));
        }
        self.snapshot = Some(snapshot);
        let read = usage_read(&quotas);
        self.state = SnapshotState::Usage(quotas);
        Ok(Next::Call(read))
    }

    fn usage(&mut self, quotas: &[(String, u64)], result: IoResult) -> SnapshotTurn {
        let mut snapshot = self.snapshot.take().ok_or(RoutingInputsError::Incomplete)?;
        let values = usage_values(result).map_err(RoutingInputsError::BackendUsage)?;
        snapshot.catalog = full_catalog(snapshot.catalog.clone(), quotas, &values)
            .map_err(RoutingInputsError::BackendUsage)?;
        Ok(Next::Done(snapshot))
    }
}

impl Operation for SnapshotOperation {
    type Output = RoutingSnapshot;
    type Error = RoutingInputsError;

    fn first(&mut self) -> SnapshotTurn {
        match std::mem::replace(&mut self.state, SnapshotState::Bucket) {
            SnapshotState::Init(BucketRules::Named(bucket)) => {
                Ok(Next::Call(IoCall::Read(vec![(
                    S3_BUCKET_KEYSPACE.to_string(),
                    bucket.into_bytes().into(),
                )])))
            }
            SnapshotState::Init(BucketRules::Known { rules, compression }) => {
                self.rules(rules, compression)
            }
            _ => Err(RoutingInputsError::Incomplete),
        }
    }

    fn next(&mut self, result: IoResult) -> SnapshotTurn {
        match std::mem::replace(&mut self.state, SnapshotState::Done) {
            SnapshotState::Bucket => {
                let [value] = values::<1>(result).map_err(RecordReadError::from)?;
                let info = value
                    .map(|value| BucketInfo::from_bytes(value.as_ref()))
                    .transpose()
                    .map_err(RecordReadError::from)?;
                let (rules, compression) = info
                    .map(|info| (info.storage_routing, info.compression))
                    .unwrap_or_default();
                self.rules(rules, compression)
            }
            SnapshotState::Group => {
                let inputs = result.child::<GroupRoutingInputs, GroupInputsError>()?;
                self.group(inputs)
            }
            SnapshotState::Usage(quotas) => self.usage(&quotas, result),
            SnapshotState::Init(_) | SnapshotState::Done => Err(RoutingInputsError::Incomplete),
        }
    }
}

/// Node routing whose capped backends already carry their fullness, for the background writers
/// that build their own snapshot later. Replication reads the same catalog, so an unreadable
/// counter refuses it too and it retries.
#[derive(Debug, PartialEq)]
pub struct QuotaOperation {
    routing: Option<NodeRouting>,
    quotas: Vec<(String, u64)>,
}

impl QuotaOperation {
    pub fn new(routing: NodeRouting) -> Self {
        Self {
            quotas: routing.catalog.quotas(),
            routing: Some(routing),
        }
    }
}

impl Operation for QuotaOperation {
    type Output = NodeRouting;
    type Error = RoutingInputsError;

    fn first(&mut self) -> Result<Next<NodeRouting>, RoutingInputsError> {
        if self.quotas.is_empty() {
            let routing = self.routing.take().ok_or(RoutingInputsError::Incomplete)?;
            return Ok(Next::Done(routing));
        }
        Ok(Next::Call(usage_read(&self.quotas)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<NodeRouting>, RoutingInputsError> {
        let routing = self.routing.take().ok_or(RoutingInputsError::Incomplete)?;
        let values = usage_values(result).map_err(RoutingInputsError::BackendUsage)?;
        let catalog = full_catalog(routing.catalog.clone(), &self.quotas, &values)
            .map_err(RoutingInputsError::BackendUsage)?;
        Ok(Next::Done(NodeRouting { catalog, ..routing }))
    }
}

/// Stored bytes of one backend, summed over its counter shards. A missing row reads as zero, so a
/// node whose counters were never built reports no usage; an unreadable shard is an error.
#[derive(Debug, PartialEq)]
pub struct UsageOperation {
    backend: BackendRef,
}

impl UsageOperation {
    pub fn new(backend: BackendRef) -> Self {
        Self { backend }
    }
}

impl Operation for UsageOperation {
    type Output = u64;
    type Error = RecordReadError;

    fn first(&mut self) -> Result<Next<u64>, RecordReadError> {
        Ok(Next::Call(batch_read(usage_backend_keys(&self.backend))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<u64>, RecordReadError> {
        Ok(Next::Done(used_bytes(&usage_values(result)?)?))
    }
}

/// One batch read of every counter shard of the capped backends, backend by backend.
fn usage_read(quotas: &[(String, u64)]) -> IoCall {
    batch_read(
        quotas
            .iter()
            .flat_map(|(name, _)| usage_backend_keys(&BackendRef::Node(name.clone())))
            .collect(),
    )
}

fn batch_read(keys: Vec<Vec<u8>>) -> IoCall {
    let reads = keys
        .into_iter()
        .map(|key| (USAGE_STATS_KEYSPACE.to_string(), key.into()))
        .collect();
    IoCall::Read(reads)
}

fn usage_values(result: IoResult) -> Result<Vec<Option<Value>>, RecordReadError> {
    match result {
        IoResult::Values(values) => Ok(values),
        _ => Err(RecordReadError::Unexpected),
    }
}

fn used_bytes(values: &[Option<Value>]) -> Result<u64, RecordReadError> {
    let mut total = 0u64;
    for value in values.iter().flatten() {
        total = total.saturating_add(UsageCounters::from_bytes(value.as_ref())?.stored_bytes);
    }
    Ok(total)
}

/// Freezes each capped backend's fullness for one request, exactly like the group quota ceiling.
/// Concurrent writes can overshoot by their own bytes; an unreadable counter fails the caller.
fn full_catalog(
    mut catalog: BackendCatalog,
    quotas: &[(String, u64)],
    values: &[Option<Value>],
) -> Result<BackendCatalog, RecordReadError> {
    let mut rest = values;
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
