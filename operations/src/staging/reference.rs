//! Materializes a staged source as a reference version that points at the source bytes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::SystemTime;

use crate::blob::records::{HeadAliasContext, build_transition_effects, write_version_effect};
use crate::connectors::descriptor::{build_source_binding, build_source_descriptor};
use crate::connectors::repository::{
    StorageReadError, connector_reference_write, parse_secret_read, read_secret_effect,
    source_connector_key,
};
use crate::connectors::resolver::secret_fingerprint;
use crate::distribution::guard::{GuardError, authority_reads, write_epoch};
use crate::distribution::inventory::{
    AddedVersion, entry_of, stamp_head, stamp_version, version_written,
};
use crate::s3::purge_fence::{PurgeFenceError, check_fence_row, fence_key};
use crate::s3::usage::{
    RowChange, UsageCounterUpdate, UsageUpdateError, schedule_snapshot_publish,
};
use crate::staging::head_source::{
    HeadSourceError, HeadSourceInput, HeadSourceOperation, HeadSourceResult,
};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, PURGE_FENCE_KEYSPACE, S3_BUCKET_KEYSPACE,
    SOURCE_INDEX_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::plugin_runs::SourceEvent;
use aruna_core::plugin_runs::journaled;
use aruna_core::rows::{read_row, value, values};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::execution::staging::{
    SourceMirror, StagingStrategy, VersionSourceBinding,
};
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::{PlacementPolicyError, PlacementPolicyRef};
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::distribution::hint::{SetHint, with_hint};
use aruna_core::structs::storage::distribution::source_root::RootEntry;
use aruna_core::structs::storage::distribution::{DistributionBinding, OriginalEpoch};
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_core::structs::storage::replication_queue::build_live_obligation;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{GroupId, Key, Value};
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializeReferenceInput {
    pub group_id: GroupId,
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub connector_id: Ulid,
    pub source_path: String,
    pub bucket: String,
    pub key: String,
    pub expected_bucket: BucketInfo,
    /// Refs the reference carries over from an object it derives from; they
    /// are unioned with the bucket default and never dropped.
    pub inherited_policies: Vec<PlacementPolicyRef>,
    /// The writer's path restrictions, which its replication obligation keeps.
    pub restrictions: Option<Vec<PathRestriction>>,
    /// Further sources of the same file: each connector of the group with the file's path there.
    pub mirrors: Vec<(SourceConnector, String)>,
    /// The import job that registers the file, recorded in its origin.
    pub job_id: Option<Ulid>,
}

#[derive(Debug, PartialEq)]
pub struct MaterializeReferenceResult {
    pub connector: SourceConnector,
    pub source_metadata: SourceMetadata,
    pub version_source: VersionSourceBinding,
    pub version_id: Ulid,
}

#[derive(Debug, Error, PartialEq)]
pub enum MaterializeReferenceError {
    #[error(transparent)]
    Head(#[from] HeadSourceError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Usage(#[from] UsageUpdateError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error(transparent)]
    Purge(#[from] PurgeFenceError),
    #[error(transparent)]
    Distribution(#[from] GuardError),
}

impl From<IoError> for MaterializeReferenceError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// Heads the staged source and records it as a reference version: the head's connector and
/// its mirrors are re-checked when the version commits.
#[derive(Debug, PartialEq)]
pub struct MaterializeReferenceOperation {
    input: Option<MaterializeReferenceInput>,
    now: SystemTime,
    fresh: Ulid,
    /// What the head found, once the reference write runs.
    found: Option<(SourceConnector, SourceMetadata, VersionSourceBinding)>,
}

impl MaterializeReferenceOperation {
    /// `now` stamps the version and `fresh` names it unless the bucket's set hints another id.
    pub fn new(input: MaterializeReferenceInput, now: SystemTime, fresh: Ulid) -> Self {
        Self {
            input: Some(input),
            now,
            fresh,
            found: None,
        }
    }

    /// Builds the reference write from the head.
    fn headed(&mut self, head: HeadSourceResult) -> Result<IoCall, MaterializeReferenceError> {
        let input = self.input.take().ok_or(StorageError::InvalidEffect)?;
        let mut version_source = build_source_binding(
            StagingStrategy::Reference,
            &head.connector,
            &head.metadata,
            input.source_path,
            Some(input.node_id),
            Some(head.connector.connector_id),
        );
        for (connector, path) in &input.mirrors {
            let mut descriptor = build_source_descriptor(
                connector,
                &head.metadata,
                path.clone(),
                Some(input.node_id),
            );
            // A version id names one server's copy, never another's.
            descriptor.version_selector = None;
            version_source.mirrors.push(SourceMirror {
                descriptor,
                connector_id: Some(connector.connector_id),
            });
        }
        let mirror_guards = (input.mirrors.into_iter()).map(|(connector, _)| connector);
        let write = ReferenceWrite {
            preassigned_version_id: None,
            group_id: input.group_id,
            user_id: input.user_id,
            realm_id: input.realm_id,
            node_id: input.node_id,
            bucket: input.bucket,
            key: input.key,
            expected_bucket: Some(input.expected_bucket),
            version_source: version_source.clone(),
            metadata: head.metadata.clone(),
            inherited_policies: input.inherited_policies,
            restrictions: input.restrictions,
            connector_guard: Some((head.connector.clone(), head.secret_fingerprint)),
            mirror_guards: mirror_guards.collect(),
            job_id: input.job_id,
        };
        self.found = Some((head.connector, head.metadata, version_source));
        let write = WriteReferenceOperation::new(write, self.now, self.fresh);
        Ok(IoCall::child(write))
    }
}

impl Operation for MaterializeReferenceOperation {
    type Output = MaterializeReferenceResult;
    type Error = MaterializeReferenceError;

    fn first(&mut self) -> Result<Next<MaterializeReferenceResult>, MaterializeReferenceError> {
        let input = self.input.as_ref().ok_or(StorageError::InvalidEffect)?;
        let head = HeadSourceOperation::new(HeadSourceInput {
            group_id: input.group_id,
            connector_id: input.connector_id,
            source_path: input.source_path.clone(),
        });
        Ok(Next::Call(IoCall::child(head)))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<MaterializeReferenceResult>, MaterializeReferenceError> {
        let Some((connector, source_metadata, version_source)) = self.found.take() else {
            let head = result.child::<HeadSourceResult, HeadSourceError>()??;
            return Ok(Next::Call(self.headed(head)?));
        };
        let written = result.child::<(Ulid, bool), MaterializeReferenceError>()?;
        let (version_id, _) = written?;
        Ok(Next::Done(MaterializeReferenceResult {
            connector,
            source_metadata,
            version_source,
            version_id,
        }))
    }
}

/// One reference version to record: freshly resolved through a connector, or
/// cloned from a version that already carries the binding.
#[derive(Clone, Debug, PartialEq)]
pub struct ReferenceWrite {
    pub preassigned_version_id: Option<Ulid>,
    pub group_id: GroupId,
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub bucket: String,
    pub key: String,
    /// The bucket as the caller saw it; `None` only checks the group.
    pub expected_bucket: Option<BucketInfo>,
    pub version_source: VersionSourceBinding,
    pub metadata: SourceMetadata,
    /// Refs unioned with the bucket default and never dropped.
    pub inherited_policies: Vec<PlacementPolicyRef>,
    /// The connector the binding was resolved through, re-checked at commit.
    pub connector_guard: Option<(SourceConnector, Option<[u8; 16]>)>,
    /// The connectors of further sources, re-checked at commit as well.
    pub mirror_guards: Vec<SourceConnector>,
    /// The writer's path restrictions, which its replication obligation keeps.
    pub restrictions: Option<Vec<PathRestriction>>,
    /// The system job that writes the reference, recorded in its origin.
    pub job_id: Option<Ulid>,
}

/// What the pending call of a reference write answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteState {
    Fence,
    Bucket,
    Authority,
    Connector,
    Secret,
    Mirror,
    Planned,
    Pointer,
    Current,
    Rows,
    Usage,
}

type WriteNext = Result<Next<(Ulid, bool)>, MaterializeReferenceError>;

/// Writes one reference version in one transaction. Outputs the version id and whether
/// anything changed: the same binding with the same observation at the head is kept.
#[derive(Debug, PartialEq)]
pub struct WriteReferenceOperation {
    write: ReferenceWrite,
    now: SystemTime,
    fresh: Ulid,
    state: WriteState,
    policies: Vec<PlacementPolicyRef>,
    binding: Option<DistributionBinding>,
    epoch: OriginalEpoch,
    version_id: Ulid,
    mirror: usize,
    pointer: Option<CurrentVersionPointer>,
    rows: VecDeque<Effect>,
    usage: Option<UsageCounterUpdate>,
}

impl WriteReferenceOperation {
    /// `now` stamps the version and `fresh` names it unless the bucket's set hints another id.
    pub fn new(write: ReferenceWrite, now: SystemTime, fresh: Ulid) -> Self {
        Self {
            write,
            now,
            fresh,
            state: WriteState::Fence,
            policies: Vec::new(),
            binding: None,
            epoch: OriginalEpoch::default(),
            version_id: fresh,
            mirror: 0,
            pointer: None,
            rows: VecDeque::new(),
            usage: None,
        }
    }

    fn call(&mut self, state: WriteState, call: IoCall) -> WriteNext {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn read(&mut self, state: WriteState, key_space: &str, key: impl Into<Key>) -> WriteNext {
        self.call(state, read_row(key_space, key))
    }

    /// Ends with `result`; a changed head wakes the usage snapshot after the commit, and a
    /// missed wake only delays that snapshot.
    fn finish(&self, result: (Ulid, bool)) -> WriteNext {
        if !result.1 {
            return Ok(Next::Done(result));
        }
        Ok(Next::Then {
            output: result,
            calls: vec![IoCall::Effect(Box::new(schedule_snapshot_publish()))],
        })
    }

    /// The bucket as read in the transaction must be the one the caller saw.
    fn bucket_read(&mut self, value: Option<Value>) -> WriteNext {
        let current = (value.as_ref())
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?;
        let write = &self.write;
        let unchanged = match write.expected_bucket.as_ref() {
            Some(expected) => {
                expected.group_id == write.group_id
                    && current.as_ref().map(BucketInfo::identity) == Some(expected.identity())
            }
            None => (current.as_ref()).is_some_and(|bucket| bucket.group_id == write.group_id),
        };
        if !unchanged {
            return Err(StorageError::TransactionConflict.into());
        }
        self.binding = (current.as_ref()).and_then(|bucket| bucket.distribution.clone());
        self.policies = current
            .map(|bucket| bucket.placement_policies)
            .unwrap_or_default();
        match self.binding.as_ref() {
            // Only the coordinator of the key's range writes, at its epoch; the read in the
            // write transaction conflicts with a fence set meanwhile.
            Some(binding) => {
                let reads = authority_reads(binding.set_id);
                self.call(WriteState::Authority, IoCall::Read(reads))
            }
            None => self.named(),
        }
    }

    /// Names the version and starts the connector checks.
    fn named(&mut self) -> WriteNext {
        let hint = self.binding.as_ref().map(DistributionBinding::hint);
        if let (Some(hint), Some(planned)) = (hint, self.write.preassigned_version_id)
            && SetHint::of_version(planned) != hint
        {
            return Err(StorageError::WriteError(
                "planned reference version lacks the set hint".into(),
            )
            .into());
        }
        self.version_id = match (self.write.preassigned_version_id, hint) {
            (Some(planned), _) => planned,
            (None, Some(hint)) => with_hint(self.fresh, hint),
            (None, None) => self.fresh,
        };
        let inherited = self.write.inherited_policies.iter().copied();
        self.policies.extend(inherited);
        match self.write.connector_guard.as_ref() {
            Some((connector, _)) => {
                let key = source_connector_key(connector.group_id, connector.connector_id);
                self.read(WriteState::Connector, SOURCE_INDEX_KEYSPACE, key)
            }
            None => self.next_mirror(),
        }
    }

    /// Re-reads the next mirror connector, or goes on to the head once all are checked.
    fn next_mirror(&mut self) -> WriteNext {
        let Some(connector) = self.write.mirror_guards.get(self.mirror) else {
            return self.versions();
        };
        let key = source_connector_key(connector.group_id, connector.connector_id);
        self.mirror += 1;
        self.read(WriteState::Mirror, SOURCE_INDEX_KEYSPACE, key)
    }

    /// A planned version is read first; otherwise the current head.
    fn versions(&mut self) -> WriteNext {
        let (bucket, key) = (&self.write.bucket, &self.write.key);
        match self.write.preassigned_version_id {
            Some(_) => {
                let row = VersionKey::new(bucket, key, self.version_id).to_bytes()?;
                self.read(WriteState::Planned, BLOB_VERSIONS_KEYSPACE, row)
            }
            None => self.pointer(),
        }
    }

    fn pointer(&mut self) -> WriteNext {
        let head = BlobHeadKey::new(&self.write.bucket, &self.write.key).to_bytes()?;
        self.read(WriteState::Pointer, BLOB_HEAD_KEYSPACE, head)
    }

    /// Whether `version` already references the same source with the same observation.
    fn same_reference(&self, version: &BlobVersion) -> bool {
        matches!(
            &version.state,
            BlobVersionState::Reference { source, cached_metadata, .. }
                if RootEntry::same_source(source, &self.write.version_source)
                    && source_metadata_matches(cached_metadata, &self.write.metadata)
        )
    }

    /// The rows of a new head version, in write order, and its usage update.
    fn plan_rows(&mut self, existing: Option<BlobVersion>) -> WriteNext {
        let write = &self.write;
        // An offered directory observation is charged only while it is the head.
        let released =
            (existing.as_ref()).and_then(|version| version.offered_size(&write.bucket, &write.key));
        let was_live = existing.is_some_and(|version| !version.is_deleted());
        let version_id = self.version_id;
        let next_pointer = CurrentVersionPointer::next_for(self.pointer.as_ref(), version_id)?;
        let next_pointer = stamp_head(next_pointer, self.binding.as_ref(), self.epoch);
        let generation = next_pointer.generation;
        let alias = HeadAliasContext::new(
            write.realm_id,
            write.group_id,
            write.node_id,
            &write.bucket,
            &write.key,
        );
        let mut rows: VecDeque<Effect> =
            build_transition_effects(&alias, Some(next_pointer), None, None)?
                .into_iter()
                .collect();
        let version_key = VersionKey::new(&write.bucket, &write.key, version_id);
        let version = BlobVersion::reference(
            write.version_source.clone(),
            write.metadata.clone(),
            self.now,
            write.user_id,
            self.now,
        )
        .with_origin(Some(VersionOrigin::Source {
            job_id: write.job_id,
        }))
        .with_policies(std::mem::take(&mut self.policies))?;
        let version = stamp_version(version, self.binding.as_ref(), generation, self.epoch);
        let now_ms = self
            .now
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |since| {
                u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
            });
        let uploaded = SourceEvent::data_uploaded(
            (write.realm_id, write.group_id, write.node_id),
            (&write.bucket, &write.key),
            version_id,
            now_ms,
        );
        let effect = write_version_effect(&version_key, &version, None)?;
        rows.push_back(journaled(effect, &uploaded)?);
        for (key_space, key, value) in connector_reference_write(&version, &version_key)? {
            rows.push_back(Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            }));
        }
        // The obligation commits with the version, so its fan-out survives a crash.
        let auth = AuthContext {
            user_id: write.user_id,
            realm_id: write.realm_id,
            path_restrictions: write.restrictions.clone(),
            session: None,
        };
        rows.push_back(build_live_obligation(
            write.node_id,
            auth,
            write.bucket.clone(),
            write.key.clone(),
            version_id,
            false,
            None,
        )?);
        let referenced_bytes = write.metadata.content_length;
        let added = AddedVersion {
            version_id,
            epoch: version.original_epoch,
            generation,
            entry: entry_of(&version, referenced_bytes),
        };
        let inventory = version_written(self.binding.as_ref(), &write.key, added, true);
        let usage = UsageCounterUpdate::for_group(
            write.group_id,
            UsageDelta {
                objects: if was_live { 0 } else { 1 },
                referenced_bytes: i128::from(referenced_bytes) - i128::from(released.unwrap_or(0)),
                ..Default::default()
            },
        )
        .in_bucket(&write.bucket, RowChange::AddedVersion)
        .with_inventory(inventory);
        self.usage = (!usage.is_noop()).then_some(usage);
        self.rows = rows;
        self.next_row()
    }

    /// Writes the next planned row, then runs the usage update, then commits.
    fn next_row(&mut self) -> WriteNext {
        if let Some(row) = self.rows.pop_front() {
            return self.call(WriteState::Rows, IoCall::from(row));
        }
        match self.usage.as_mut() {
            Some(usage) => {
                let call = usage.start_calls().into_call();
                self.call(WriteState::Usage, call)
            }
            None => self.finish((self.version_id, true)),
        }
    }

    fn existing(value: Option<Value>) -> Result<Option<BlobVersion>, ConversionError> {
        (value.as_ref())
            .map(|value| BlobVersion::from_bytes(value.as_ref()))
            .transpose()
    }
}

/// The connector record must be unchanged since it was resolved.
fn guard_record(
    resolved: &SourceConnector,
    value: Option<Value>,
) -> Result<(), MaterializeReferenceError> {
    let current = (value.as_ref())
        .map(|value| SourceConnector::from_bytes(value.as_ref()))
        .transpose()?;
    match current.as_ref() == Some(resolved) {
        true => Ok(()),
        false => Err(StorageError::TransactionConflict.into()),
    }
}

impl Operation for WriteReferenceOperation {
    type Output = (Ulid, bool);
    type Error = MaterializeReferenceError;

    fn first(&mut self) -> WriteNext {
        let fence = read_row(PURGE_FENCE_KEYSPACE, fence_key(&self.write.bucket));
        Ok(Next::Call(fence))
    }

    fn next(&mut self, result: IoResult) -> WriteNext {
        let (bucket, key) = (self.write.bucket.clone(), self.write.key.clone());
        match self.state {
            WriteState::Fence => {
                check_fence_row(value(result)?.as_deref(), &bucket, &key)?;
                self.read(WriteState::Bucket, S3_BUCKET_KEYSPACE, bucket.into_bytes())
            }
            WriteState::Bucket => self.bucket_read(value(result)?),
            WriteState::Authority => {
                let [record, fence] = values::<2>(result)?;
                let binding = self.binding.as_ref().ok_or(StorageError::InvalidEffect)?;
                self.epoch = write_epoch(binding, &key, record.as_deref(), fence.as_deref())?;
                self.named()
            }
            WriteState::Connector => {
                let (connector, _) =
                    (self.write.connector_guard.as_ref()).ok_or(StorageError::InvalidEffect)?;
                guard_record(connector, value(result)?)?;
                let read = IoCall::from(read_secret_effect(connector.connector_id, None));
                self.call(WriteState::Secret, read)
            }
            WriteState::Secret => {
                let (connector, fingerprint) =
                    (self.write.connector_guard.as_ref()).ok_or(StorageError::InvalidEffect)?;
                let current = match parse_secret_read(result.event()?, connector.connector_id) {
                    Ok(secret) => secret,
                    Err(StorageReadError::Storage(error)) => return Err(error.into()),
                    Err(StorageReadError::Conversion(error)) => return Err(error.into()),
                };
                if current.as_ref().map(secret_fingerprint) != *fingerprint {
                    return Err(StorageError::TransactionConflict.into());
                }
                self.next_mirror()
            }
            WriteState::Mirror => {
                let checked = self.mirror.checked_sub(1);
                let connector = (checked.and_then(|index| self.write.mirror_guards.get(index)))
                    .ok_or(StorageError::InvalidEffect)?;
                guard_record(connector, value(result)?)?;
                self.next_mirror()
            }
            WriteState::Planned => match Self::existing(value(result)?)? {
                Some(existing) if self.same_reference(&existing) => {
                    self.finish((self.version_id, false))
                }
                Some(_) => Err(StorageError::WriteError(
                    "planned reference version changed".into(),
                )
                .into()),
                None => self.pointer(),
            },
            WriteState::Pointer => {
                self.pointer = (value(result)?.as_ref())
                    .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
                    .transpose()?;
                match self.pointer.as_ref() {
                    Some(pointer) => {
                        let row = VersionKey::new(&bucket, &key, pointer.version_id).to_bytes()?;
                        self.read(WriteState::Current, BLOB_VERSIONS_KEYSPACE, row)
                    }
                    None => self.plan_rows(None),
                }
            }
            WriteState::Current => {
                let existing = Self::existing(value(result)?)?;
                let kept = (self.write.preassigned_version_id.is_none())
                    .then(|| {
                        existing
                            .as_ref()
                            .filter(|version| self.same_reference(version))
                    })
                    .flatten()
                    .and(self.pointer.as_ref().map(|pointer| pointer.version_id));
                match kept {
                    Some(version_id) => self.finish((version_id, false)),
                    None => self.plan_rows(existing),
                }
            }
            WriteState::Rows => match result.event()? {
                Event::Storage(
                    StorageEvent::WriteResult { .. }
                    | StorageEvent::BatchWriteResult { .. }
                    | StorageEvent::DeleteResult { .. },
                ) => self.next_row(),
                other => Err(StorageError::ReadError(format!(
                    "unexpected event {} for a reference row",
                    other.label()
                ))
                .into()),
            },
            WriteState::Usage => {
                let usage = self.usage.as_mut().ok_or(StorageError::InvalidEffect)?;
                match usage.step_calls(result)? {
                    Some(call) => Ok(Next::Call(call.into_call())),
                    None => self.finish((self.version_id, true)),
                }
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

fn source_metadata_matches(left: &SourceMetadata, right: &SourceMetadata) -> bool {
    left.content_length == right.content_length
        && left.content_type == right.content_type
        && left.etag == right.etag
        && left.last_modified == right.last_modified
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::repository::{delete_secret_effect, write_secret_effect};
    use crate::driver::{DriverContext, drive};
    use crate::node::usage_rebuild::RebuildStatsOperation;
    use crate::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use crate::tests::staging::{create_http_connector, create_test_bucket, setup_driver_context};
    use aruna_core::effects::StorageEffect;
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, PATHS_INDEX_KEYSPACE, PURGE_FENCE_KEYSPACE,
        REPLICATION_OBLIGATION_KEYSPACE, SOURCE_INDEX_KEYSPACE, USAGE_STATS_KEYSPACE,
    };
    use aruna_core::node_vault::NodeVaultKey;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::execution::job::JobId;
    use aruna_core::structs::execution::source_connector::{
        SourceConnectorKind, SourceConnectorSecret,
    };
    use aruna_core::structs::storage::blob::{
        BlobHeadKey, BlobVersion, CurrentVersionPointer, HashIndex,
    };
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::replication_queue::{
        LiveObligationRecord, live_obligation_key,
    };
    use aruna_core::structs::storage::routing::RoutingSnapshot;
    use aruna_core::structs::storage::storage_purge::{StoragePurgeFence, StoragePurgeScope};
    use aruna_core::structs::storage::usage::{UsageCounters, global_group_key, usage_group_key};
    use aruna_storage::storage;
    use axum::{Router, routing::get};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::{TempDir, tempdir};
    use tokio::net::TcpListener;

    async fn stage(
        context: &DriverContext,
        input: MaterializeReferenceInput,
    ) -> Result<MaterializeReferenceResult, MaterializeReferenceError> {
        let operation =
            MaterializeReferenceOperation::new(input, SystemTime::now(), Ulid::generate());
        drive(operation, context).await
    }

    async fn write_version(
        context: &DriverContext,
        write: ReferenceWrite,
    ) -> Result<(Ulid, bool), MaterializeReferenceError> {
        let operation = WriteReferenceOperation::new(write, SystemTime::now(), Ulid::generate());
        drive(operation, context).await
    }

    fn test_context() -> (TempDir, DriverContext) {
        let tempdir = tempdir().expect("tempdir must be created");
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        storage_handle.open_vault(NodeVaultKey::random());
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        (tempdir, context)
    }

    fn connector() -> SourceConnector {
        SourceConnector::new(
            Ulid::from_bytes([2u8; 16]),
            Ulid::from_bytes([1u8; 16]),
            "s3-source".to_string(),
            SourceConnectorKind::S3,
            HashMap::from([
                ("bucket".to_string(), "data".to_string()),
                ("region".to_string(), "eu-central-1".to_string()),
            ]),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Default::default(),
        )
    }

    fn secret(value: &str) -> SourceConnectorSecret {
        SourceConnectorSecret::new(
            Ulid::from_bytes([2u8; 16]),
            HashMap::from([("access_key_id".to_string(), value.to_string())]),
            SystemTime::UNIX_EPOCH,
        )
        .expect("secret must be present")
    }

    async fn write_connector(context: &DriverContext, connector: &SourceConnector) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: SOURCE_INDEX_KEYSPACE.to_string(),
                key: source_connector_key(connector.group_id, connector.connector_id),
                value: connector.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn write_secret(context: &DriverContext, secret: &SourceConnectorSecret) {
        let event = context
            .storage_handle
            .send_effect(write_secret_effect(secret, None).unwrap())
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn delete_connector(context: &DriverContext, connector: &SourceConnector) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Delete {
                key_space: SOURCE_INDEX_KEYSPACE.to_string(),
                key: source_connector_key(connector.group_id, connector.connector_id),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::DeleteResult { .. })
        ));
    }

    async fn delete_secret(context: &DriverContext, connector_id: Ulid) {
        let event = context
            .storage_handle
            .send_effect(delete_secret_effect(connector_id, None))
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::DeleteResult { .. })
        ));
    }

    /// Writes a reference to `bucket-a` of `group_id` through the given connector and bucket guards.
    async fn guarded(
        context: &DriverContext,
        group_id: Ulid,
        connector_guard: Option<(SourceConnector, Option<[u8; 16]>)>,
        expected_bucket: Option<BucketInfo>,
    ) -> Result<(), MaterializeReferenceError> {
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let metadata = SourceMetadata {
            content_length: 1,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        };
        let resolved = connector();
        let version_source = build_source_binding(
            StagingStrategy::Reference,
            &resolved,
            &metadata,
            "a".to_string(),
            None,
            Some(resolved.connector_id),
        );
        let write = ReferenceWrite {
            job_id: None,
            preassigned_version_id: None,
            group_id,
            user_id: aruna_core::UserId::local(Ulid::from_bytes([3; 16]), realm_id),
            realm_id,
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            bucket: "bucket-a".into(),
            key: "object.txt".into(),
            expected_bucket,
            version_source,
            metadata,
            inherited_policies: Vec::new(),
            connector_guard,
            mirror_guards: Vec::new(),
            restrictions: None,
        };
        write_version(context, write).await.map(|_| ())
    }

    /// Stores `bucket-a` for `group_id`, so only the guard under test can refuse the write.
    fn seeded_bucket(group_id: Ulid) -> BucketInfo {
        let realm_id = RealmId::from_bytes([6u8; 32]);
        BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::local(Ulid::from_bytes([3; 16]), realm_id),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        }
    }

    async fn seed_bucket(context: &DriverContext, group_id: Ulid) {
        let bucket = seeded_bucket(group_id);
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: b"bucket-a".to_vec().into(),
                value: bucket.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    fn assert_conflict(result: Result<(), MaterializeReferenceError>) {
        assert_eq!(
            result,
            Err(MaterializeReferenceError::Storage(
                StorageError::TransactionConflict,
            )),
        );
    }

    async fn read_value(
        context: &DriverContext,
        key_space: &str,
        key: Vec<u8>,
    ) -> Option<aruna_core::types::Value> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: key.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read result");
        };

        value
    }

    async fn read_usage_counters(context: &DriverContext, key: Vec<u8>) -> UsageCounters {
        read_value(context, USAGE_STATS_KEYSPACE, key)
            .await
            .map(|value| UsageCounters::from_bytes(value.as_ref()).unwrap())
            .unwrap_or_default()
    }

    async fn spawn_reference_server(body: &'static str) -> (tokio::task::JoinHandle<()>, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let app =
                Router::new().route(
                    "/folder/file.txt",
                    get(move || async move {
                        ([("etag", "etag-1"), ("content-type", "text/plain")], body)
                    }),
                );
            axum::serve(listener, app).await.unwrap();
        });
        (server, format!("http://{addr}"))
    }

    #[tokio::test]
    async fn guard_rejects_deleted() {
        let (_tempdir, context) = test_context();
        let connector = connector();
        let resolved_secret = secret("old-key");
        write_connector(&context, &connector).await;
        write_secret(&context, &resolved_secret).await;

        delete_connector(&context, &connector).await;
        seed_bucket(&context, connector.group_id).await;
        let guard = (
            connector.clone(),
            Some(secret_fingerprint(&resolved_secret)),
        );
        assert_conflict(guarded(&context, connector.group_id, Some(guard), None).await);
    }

    #[tokio::test]
    async fn guard_rejects_changed() {
        let (_tempdir, context) = test_context();
        let connector = connector();
        let resolved_secret = secret("old-key");
        write_connector(&context, &connector).await;
        write_secret(&context, &resolved_secret).await;

        write_secret(&context, &secret("new-key")).await;
        seed_bucket(&context, connector.group_id).await;
        let guard = (
            connector.clone(),
            Some(secret_fingerprint(&resolved_secret)),
        );
        assert_conflict(guarded(&context, connector.group_id, Some(guard), None).await);
    }

    #[tokio::test]
    async fn guard_rejects_removed() {
        let (_tempdir, context) = test_context();
        let connector = connector();
        let resolved_secret = secret("old-key");
        write_connector(&context, &connector).await;
        write_secret(&context, &resolved_secret).await;

        delete_secret(&context, connector.connector_id).await;
        seed_bucket(&context, connector.group_id).await;
        let guard = (
            connector.clone(),
            Some(secret_fingerprint(&resolved_secret)),
        );
        assert_conflict(guarded(&context, connector.group_id, Some(guard), None).await);
    }

    #[tokio::test]
    async fn guard_rejects_added() {
        let (_tempdir, context) = test_context();
        let connector = connector();
        write_connector(&context, &connector).await;

        write_secret(&context, &secret("new-key")).await;
        seed_bucket(&context, connector.group_id).await;
        let guard = (connector.clone(), None);
        assert_conflict(guarded(&context, connector.group_id, Some(guard), None).await);
    }

    #[tokio::test]
    async fn bucket_rejects_recreate() {
        let (_tempdir, context) = test_context();
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let group_id = Ulid::generate();
        let expected = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::local(Ulid::generate(), realm_id),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let recreated = BucketInfo {
            created_at: SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1),
            ..expected.clone()
        };
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: b"bucket-a".to_vec().into(),
                value: recreated.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        assert_conflict(guarded(&context, group_id, None, Some(expected)).await);
    }

    #[tokio::test]
    async fn reference_keeps_history() {
        // The prior version's hash-path entry survives while the new reference
        // version is written.
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let group_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([9u8; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let expected_bucket = create_test_bucket(context, group_id, user_id, "bucket-a").await;

        let initial = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id,
                group_id,
                realm_id,
                node_id,
                request: PutObjectInput {
                    bucket: "bucket-a".to_string(),
                    key: "object.txt".to_string(),
                    content_length: Some(5),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &b"hello"[..],
                    ))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
                version_source: None,
                preassigned_version_id: None,
                quota_ceiling: None,
                routing: RoutingSnapshot::single(group_id),
            }),
            context,
        )
        .await
        .unwrap();
        let initial_hash: [u8; 32] = initial.location.get_blake3().unwrap().try_into().unwrap();

        let (server, endpoint) = spawn_reference_server("ref-data").await;
        let connector = create_http_connector(context, group_id, &endpoint).await;

        let result = stage(
            context,
            MaterializeReferenceInput {
                job_id: None,
                group_id,
                user_id,
                realm_id,
                node_id,
                connector_id: connector.connector_id,
                source_path: "folder/file.txt".to_string(),
                bucket: "bucket-a".to_string(),
                key: "object.txt".to_string(),
                expected_bucket,
                inherited_policies: Vec::new(),
                restrictions: None,
                mirrors: Vec::new(),
            },
        )
        .await
        .unwrap();

        server.abort();
        let _ = server.await;

        let blob_head_value = read_value(
            context,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new("bucket-a", "object.txt")
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing blob head entry");
        assert_eq!(
            CurrentVersionPointer::from_bytes(blob_head_value.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(result.version_id, 2)
        );

        let blob_version_value = read_value(
            context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("bucket-a", "object.txt", result.version_id)
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing blob version entry");
        let blob_version = BlobVersion::from_bytes(blob_version_value.as_ref()).unwrap();
        assert!(!blob_version.is_materialized());
        assert!(!blob_version.is_deleted());
        assert_eq!(blob_version.source_binding(), Some(&result.version_source));

        let historical_hash_path = read_value(
            context,
            PATHS_INDEX_KEYSPACE,
            HashIndex::new(
                initial_hash,
                initial.version_id,
                realm_id,
                group_id,
                node_id,
                "bucket-a",
                "object.txt",
            )
            .to_bytes()
            .unwrap(),
        )
        .await
        .expect("missing historical materialized hash path entry");
        assert!(historical_hash_path.is_empty());

        let group_usage = read_usage_counters(context, usage_group_key(group_id)).await;
        assert_eq!(group_usage.objects, 1);
        assert_eq!(group_usage.logical_bytes, 5);
        assert_eq!(group_usage.referenced_bytes, 8);
        let global_usage = read_usage_counters(context, global_group_key(group_id)).await;
        assert_eq!(global_usage.objects, 1);
        assert_eq!(global_usage.logical_bytes, 5);
        assert_eq!(global_usage.referenced_bytes, 8);

        // A recount from the stored rows books the same charges.
        drive(RebuildStatsOperation::new(), context).await.unwrap();
        assert_eq!(
            read_usage_counters(context, usage_group_key(group_id)).await,
            group_usage
        );
        assert_eq!(
            read_usage_counters(context, global_group_key(group_id)).await,
            global_usage
        );
    }

    #[tokio::test]
    async fn reference_bypasses_quota() {
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let group_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([8u8; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let expected_bucket = create_test_bucket(context, group_id, user_id, "bucket-a").await;
        let (server, endpoint) = spawn_reference_server("ref-data").await;
        let connector = create_http_connector(context, group_id, &endpoint).await;

        let result = stage(
            context,
            MaterializeReferenceInput {
                job_id: None,
                group_id,
                user_id,
                realm_id,
                node_id,
                connector_id: connector.connector_id,
                source_path: "folder/file.txt".to_string(),
                bucket: "bucket-a".to_string(),
                key: "object.txt".to_string(),
                expected_bucket,
                inherited_policies: Vec::new(),
                restrictions: None,
                mirrors: Vec::new(),
            },
        )
        .await;

        server.abort();
        let _ = server.await;

        assert!(result.is_ok());
        let usage = read_usage_counters(context, usage_group_key(group_id)).await;
        assert_eq!(usage.logical_bytes, 0);
        assert_eq!(usage.referenced_bytes, 8);
    }

    #[tokio::test]
    async fn reference_obeys_fence() {
        let (_tempdir, context) = test_context();
        let fence = StoragePurgeFence {
            job_id: JobId::from_bytes([11; 16]),
            scope: StoragePurgeScope::File {
                bucket: "bucket-a".to_string(),
                key: "object.txt".to_string(),
            },
        };
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: PURGE_FENCE_KEYSPACE.to_string(),
                key: b"bucket-a".to_vec().into(),
                value: fence.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        let result = guarded(&context, connector().group_id, None, None).await;

        assert_eq!(
            result,
            Err(MaterializeReferenceError::Purge(PurgeFenceError::Suspended))
        );
    }

    #[tokio::test]
    async fn reference_reuses_version() {
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let group_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let expected_bucket = create_test_bucket(context, group_id, user_id, "bucket-a").await;
        let (server, endpoint) = spawn_reference_server("ref-data").await;
        let connector = create_http_connector(context, group_id, &endpoint).await;
        let input = MaterializeReferenceInput {
            job_id: None,
            group_id,
            user_id,
            realm_id,
            node_id,
            connector_id: connector.connector_id,
            source_path: "folder/file.txt".to_string(),
            bucket: "bucket-a".to_string(),
            key: "object.txt".to_string(),
            expected_bucket,
            inherited_policies: Vec::new(),
            restrictions: None,
            mirrors: Vec::new(),
        };

        let first = stage(context, input.clone()).await.unwrap();
        let second = stage(context, input).await.unwrap();
        server.abort();
        let _ = server.await;

        assert_eq!(second.version_id, first.version_id);
        let pointer = read_value(
            context,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new("bucket-a", "object.txt")
                .to_bytes()
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            CurrentVersionPointer::from_bytes(pointer.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(first.version_id, 1)
        );
        let usage = read_usage_counters(context, usage_group_key(group_id)).await;
        assert_eq!(usage.referenced_bytes, 8);
    }

    // An item's further sources join its binding without a version id; a further source's
    // connector that changed before the write commits refuses the write.
    #[tokio::test]
    async fn mirror_connector_guarded() {
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let group_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([7; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let bucket = create_test_bucket(context, group_id, user_id, "bucket-a").await;
        let (server, endpoint) = spawn_reference_server("ref-data").await;
        let connector = create_http_connector(context, group_id, &endpoint).await;
        let mirror = create_http_connector(context, group_id, &endpoint).await;
        let input = |key: &str, mirror: SourceConnector| MaterializeReferenceInput {
            job_id: None,
            group_id,
            user_id,
            realm_id,
            node_id,
            connector_id: connector.connector_id,
            source_path: "folder/file.txt".into(),
            bucket: "bucket-a".into(),
            key: key.into(),
            expected_bucket: bucket.clone(),
            inherited_policies: Vec::new(),
            restrictions: None,
            mirrors: vec![(mirror, "mirror/file.txt".into())],
        };
        let staged = stage(context, input("a.txt", mirror.clone()))
            .await
            .unwrap();
        let added = &staged.version_source.mirrors[0];
        assert_eq!(added.connector_id, Some(mirror.connector_id));
        assert_eq!(added.descriptor.source_path, "mirror/file.txt");
        assert_eq!(added.descriptor.version_selector, None);
        let mut replaced = mirror.clone();
        replaced.name = "replaced".to_string();
        let refused = stage(context, input("b.txt", replaced)).await;
        assert!(
            matches!(
                refused,
                Err(MaterializeReferenceError::Storage(
                    StorageError::TransactionConflict
                ))
            ),
            "{refused:?}"
        );
        server.abort();
    }

    #[tokio::test]
    async fn reference_resumes_version() {
        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let group_id = Ulid::generate();
        let realm_id = RealmId::from_bytes([7; 32]);
        let node_id = iroh::SecretKey::generate().public();
        let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
        let bucket = create_test_bucket(context, group_id, user_id, "bucket-a").await;
        let (server, endpoint) = spawn_reference_server("ref-data").await;
        let connector = create_http_connector(context, group_id, &endpoint).await;
        let original = stage(
            context,
            MaterializeReferenceInput {
                job_id: None,
                group_id,
                user_id,
                realm_id,
                node_id,
                connector_id: connector.connector_id,
                source_path: "folder/file.txt".into(),
                bucket: "bucket-a".into(),
                key: "object.txt".into(),
                expected_bucket: bucket.clone(),
                inherited_policies: Vec::new(),
                restrictions: None,
                mirrors: Vec::new(),
            },
        )
        .await
        .unwrap();
        server.abort();
        let _ = server.await;
        let planned = Ulid::generate();
        let input = |size| ReferenceWrite {
            job_id: None,
            preassigned_version_id: Some(planned),
            group_id,
            user_id,
            realm_id,
            node_id,
            bucket: "bucket-a".into(),
            key: "object.txt".into(),
            expected_bucket: Some(bucket.clone()),
            version_source: original.version_source.clone(),
            metadata: SourceMetadata {
                content_length: size,
                ..original.source_metadata.clone()
            },
            inherited_policies: Vec::new(),
            connector_guard: None,
            mirror_guards: Vec::new(),
            restrictions: None,
        };
        assert_eq!(
            write_version(context, input(8)).await.unwrap(),
            (planned, true)
        );
        assert_ne!(planned, original.version_id);
        // The written version commits with its replication obligation.
        let record = LiveObligationRecord::new(
            node_id,
            AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            "bucket-a".into(),
            "object.txt".into(),
            planned,
            false,
        );
        let key = live_obligation_key(&record).unwrap();
        let obligation = read_value(context, REPLICATION_OBLIGATION_KEYSPACE, key.to_vec()).await;
        assert!(obligation.is_some(), "the reference has no obligation");
        assert_eq!(
            write_version(context, input(8)).await.unwrap(),
            (planned, false)
        );
        assert!(write_version(context, input(9)).await.is_err());
        assert_eq!(
            read_usage_counters(context, usage_group_key(group_id))
                .await
                .referenced_bytes,
            16
        );
    }

    #[tokio::test]
    async fn reference_releases_observation() {
        // A reference written over a head observation takes its place, so the
        // observation stops being charged and a recount agrees.
        use crate::staging::offered_directory::{OfferDirectoryInput, offer_directory};
        use aruna_core::structs::execution::staging::PortableSourceDescriptor;

        let test_context = setup_driver_context().await;
        let context = &test_context.driver_context;
        let root = tempdir().unwrap();
        std::fs::write(root.path().join("data.bin"), b"offered").unwrap();
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let offer = OfferDirectoryInput {
            bucket: "offered".to_string(),
            root: root.path().to_str().unwrap().to_string(),
            group_id: Ulid::generate(),
            realm_id,
            node_id: iroh::SecretKey::generate().public(),
            user_id: aruna_core::UserId::new(Ulid::generate(), realm_id),
        };
        offer_directory(context, offer.clone()).await.unwrap();

        let write = ReferenceWrite {
            job_id: None,
            preassigned_version_id: None,
            group_id: offer.group_id,
            user_id: offer.user_id,
            realm_id,
            node_id: offer.node_id,
            bucket: offer.bucket.clone(),
            key: "data.bin".to_string(),
            expected_bucket: None,
            version_source: VersionSourceBinding {
                strategy: StagingStrategy::Reference,
                descriptor: PortableSourceDescriptor {
                    kind: SourceConnectorKind::Http,
                    public_config: HashMap::new(),
                    source_path: "folder/file.txt".to_string(),
                    version_selector: None,
                    capabilities: Vec::new(),
                    origin_node_id: None,
                    namespace: None,
                    root: None,
                },
                connector_id: Some(Ulid::generate()),
                mirrors: Vec::new(),
            },
            metadata: SourceMetadata {
                content_length: 4,
                content_type: None,
                etag: None,
                last_modified: None,
                source_version: None,
                digests: Default::default(),
            },
            inherited_policies: Vec::new(),
            connector_guard: None,
            restrictions: None,
            mirror_guards: Vec::new(),
        };
        write_version(context, write).await.unwrap();

        let usage = read_usage_counters(context, usage_group_key(offer.group_id)).await;
        assert_eq!((usage.objects, usage.referenced_bytes), (1, 4));
        drive(RebuildStatsOperation::new(), context).await.unwrap();
        assert_eq!(
            read_usage_counters(context, usage_group_key(offer.group_id)).await,
            usage
        );
    }
    fn pure_write() -> WriteReferenceOperation {
        let realm_id = RealmId::from_bytes([6u8; 32]);
        let metadata = SourceMetadata {
            content_length: 1,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        };
        let resolved = connector();
        let version_source = build_source_binding(
            StagingStrategy::Reference,
            &resolved,
            &metadata,
            "a".to_string(),
            None,
            Some(resolved.connector_id),
        );
        let write = ReferenceWrite {
            job_id: None,
            preassigned_version_id: None,
            group_id: resolved.group_id,
            user_id: aruna_core::UserId::local(Ulid::from_bytes([3; 16]), realm_id),
            realm_id,
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            bucket: "bucket-a".into(),
            key: "object.txt".into(),
            expected_bucket: None,
            version_source,
            metadata,
            inherited_policies: Vec::new(),
            connector_guard: None,
            mirror_guards: Vec::new(),
            restrictions: None,
        };
        WriteReferenceOperation::new(write, SystemTime::UNIX_EPOCH, Ulid::from_bytes([5; 16]))
    }

    // The write reads the purge fence, then the bucket, in its transaction; a missing bucket
    // is a conflict.
    #[test]
    fn missing_bucket_conflicts() {
        let mut operation = pure_write();
        assert_eq!(operation.transaction(), TransactionMode::Write);
        let fence = read_row(PURGE_FENCE_KEYSPACE, fence_key("bucket-a"));
        assert_eq!(operation.first(), Ok(Next::Call(fence)));
        let bucket = read_row(S3_BUCKET_KEYSPACE, b"bucket-a".to_vec());
        let absent = || IoResult::Values(vec![None]);
        assert_eq!(operation.next(absent()), Ok(Next::Call(bucket)));
        assert_conflict(operation.next(absent()).map(|_| ()));
    }

    fn at_head() -> WriteReferenceOperation {
        let mut operation = pure_write();
        operation.state = WriteState::Pointer;
        operation.binding = None;
        operation
    }

    // A new head writes its rows one by one, updates usage and wakes the snapshot publisher
    // after the commit.
    #[test]
    fn head_wakes_publisher() {
        let mut operation = at_head();
        let absent = IoResult::Values(vec![None]);
        assert!(matches!(
            operation.next(absent),
            Ok(Next::Call(IoCall::Effect(_)))
        ));
        let written = || {
            IoResult::Event(Box::new(Event::Storage(StorageEvent::WriteResult {
                key: Vec::new().into(),
            })))
        };
        let mut next = operation.next(written());
        while matches!(next, Ok(Next::Call(IoCall::Effect(_)))) {
            next = operation.next(written());
        }
        assert!(matches!(next, Ok(Next::Call(IoCall::AddUsage { .. }))));
        assert!(matches!(
            operation.next(IoResult::Done),
            Ok(Next::Call(IoCall::Write(_)))
        ));
        let Ok(Next::Then { output, calls }) = operation.next(IoResult::Done) else {
            panic!("expected the version and the wake");
        };
        assert_eq!(output, (Ulid::from_bytes([5; 16]), true));
        assert_eq!(calls.len(), 1);
    }

    // A head that already references the same observation keeps it and changes nothing.
    #[test]
    fn same_head_kept() {
        let mut operation = at_head();
        let held = Ulid::from_bytes([8; 16]);
        let pointer = CurrentVersionPointer::new(held).to_bytes().unwrap();
        let stored = IoResult::Values(vec![Some(pointer.into())]);
        assert!(matches!(
            operation.next(stored),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        let version = BlobVersion::reference(
            operation.write.version_source.clone(),
            operation.write.metadata.clone(),
            SystemTime::UNIX_EPOCH,
            operation.write.user_id,
            SystemTime::UNIX_EPOCH,
        );
        let current = IoResult::Values(vec![Some(version.to_bytes().unwrap().into())]);
        assert_eq!(operation.next(current), Ok(Next::Done((held, false))));
    }

    fn materialize_input() -> MaterializeReferenceInput {
        let write = pure_write().write;
        MaterializeReferenceInput {
            group_id: write.group_id,
            user_id: write.user_id,
            realm_id: write.realm_id,
            node_id: write.node_id,
            connector_id: connector().connector_id,
            source_path: "a".to_string(),
            bucket: write.bucket,
            key: write.key,
            expected_bucket: seeded_bucket(write.group_id),
            inherited_policies: Vec::new(),
            restrictions: None,
            mirrors: Vec::new(),
            job_id: None,
        }
    }

    // A materialization heads the source, then writes the reference, each as a child.
    #[test]
    fn materialize_runs_children() {
        use aruna_core::calls::ChildOutcome;
        let version_id = Ulid::from_bytes([5; 16]);
        let mut operation =
            MaterializeReferenceOperation::new(materialize_input(), SystemTime::UNIX_EPOCH, version_id);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let metadata = pure_write().write.metadata;
        let head: Result<HeadSourceResult, HeadSourceError> = Ok(HeadSourceResult {
            connector: connector(),
            secret_fingerprint: None,
            metadata: metadata.clone(),
        });
        let write = operation.next(IoResult::Child(ChildOutcome::new(head)));
        assert!(matches!(write, Ok(Next::Call(IoCall::Child(_)))));
        let written: Result<(Ulid, bool), MaterializeReferenceError> = Ok((version_id, true));
        let Ok(Next::Done(result)) = operation.next(IoResult::Child(ChildOutcome::new(written)))
        else {
            panic!("expected the materialized reference");
        };
        assert_eq!(result.version_id, version_id);
        assert_eq!(result.source_metadata, metadata);
    }

    // A failed head is the materialization's error.
    #[test]
    fn head_failure_surfaces() {
        use aruna_core::calls::ChildOutcome;
        use aruna_core::errors::SourceResolutionError;
        let mut operation = MaterializeReferenceOperation::new(
            materialize_input(),
            SystemTime::UNIX_EPOCH,
            Ulid::nil(),
        );
        operation.first().unwrap();
        let failed: Result<HeadSourceResult, HeadSourceError> =
            Err(SourceResolutionError::NotFound.into());
        assert_eq!(
            operation.next(IoResult::Child(ChildOutcome::new(failed))),
            Err(HeadSourceError::Resolve(SourceResolutionError::NotFound).into())
        );
    }
}
