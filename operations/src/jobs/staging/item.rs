//! Stages one listed item of a job: checks its bucket and access, writes it as a reference or a
//! snapshot, and queues the replication of the new version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, SystemTime};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::checksum::ExpectedChecksum;
use aruna_core::structs::execution::job::{StagingJobCheckpoint, StagingJobItem};
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::execution::staging::StagingStrategy;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::BucketInfo;
use tracing::warn;

use super::mirrors::ItemMirrorsOperation;
use super::walk::{
    authority_call, file_access, item_phase, local_ranges, record_failure, rules_call,
};
use super::{ItemFailure, PageFailure, StagingRun, pinned};
use crate::auth::permission_rules::PermissionRules;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::replication::queue::{
    BlobQueueError, LiveVersionInput, LiveVersionOperation, QueueBlobResult,
};
use crate::staging::reference::{
    MaterializeReferenceError, MaterializeReferenceInput, MaterializeReferenceOperation,
    MaterializeReferenceResult,
};
use crate::staging::snapshot::{
    MaterializeSnapshotError, MaterializeSnapshotInput, MaterializeSnapshotOperation,
    MaterializeSnapshotResult,
};
use aruna_core::errors::AuthorizationError;

/// The answer of a child that fails with anything printable, as a system failure.
fn system<T: 'static, E: ToString + 'static>(result: IoResult) -> Result<T, ItemFailure> {
    (result.child::<T, E>()?).map_err(|error| ItemFailure::System(error.to_string()))
}

#[derive(Debug, PartialEq)]
enum StageStep {
    Bucket,
    Rules,
    Mirrors,
    Quota,
    Write,
    Replicate(u64),
}

/// One item as a reference or a snapshot of its source, its replication queued. Without the
/// replication seed peers never learn about the staged version, so the item is no success then.
#[derive(Debug, PartialEq)]
pub(super) struct StageItemOperation {
    run: StagingRun,
    source_path: String,
    target_key: String,
    expected: Vec<ExpectedChecksum>,
    index: u64,
    now_ms: u64,
    bucket: Option<BucketInfo>,
    mirrors: Vec<(SourceConnector, String)>,
    step: StageStep,
}

impl StageItemOperation {
    pub(super) fn new(
        run: StagingRun,
        (source_path, target_key): (String, String),
        expected: Vec<ExpectedChecksum>,
        index: u64,
    ) -> Self {
        Self {
            run,
            source_path,
            target_key,
            expected,
            index,
            now_ms: 0,
            bucket: None,
            mirrors: Vec::new(),
            step: StageStep::Bucket,
        }
    }

    fn mirrors_or_write(&mut self) -> Result<Next<u64>, ItemFailure> {
        if self.run.spec.mirrors.is_empty() {
            return self.write();
        }
        self.step = StageStep::Mirrors;
        let mirrors = ItemMirrorsOperation::new(self.run.clone(), self.source_path.clone());
        Ok(Next::Call(IoCall::child(mirrors)))
    }

    fn bucket(&mut self) -> Result<BucketInfo, ItemFailure> {
        self.bucket.take().ok_or(ItemFailure::System(
            "the staged item lost its bucket".to_string(),
        ))
    }

    fn write(&mut self) -> Result<Next<u64>, ItemFailure> {
        let spec = self.run.spec.clone();
        match spec.strategy {
            StagingStrategy::Reference => {
                let expected_bucket = self.bucket()?;
                let now = SystemTime::UNIX_EPOCH + Duration::from_millis(self.now_ms);
                let fresh = self.run.fresh_id(self.now_ms, self.index);
                self.step = StageStep::Write;
                let input = MaterializeReferenceInput {
                    group_id: spec.group_id,
                    user_id: spec.auth_context.user_id,
                    realm_id: spec.auth_context.realm_id,
                    node_id: spec.node_id,
                    connector_id: spec.connector_id,
                    source_path: self.source_path.clone(),
                    bucket: spec.bucket.clone(),
                    key: self.target_key.clone(),
                    expected_bucket,
                    inherited_policies: Vec::new(),
                    restrictions: spec.auth_context.path_restrictions.clone(),
                    mirrors: std::mem::take(&mut self.mirrors),
                    job_id: Some(self.run.job_id.as_ulid()),
                };
                let reference = MaterializeReferenceOperation::new(input, now, fresh);
                Ok(Next::Call(IoCall::child(reference)))
            }
            StagingStrategy::Snapshot => {
                self.step = StageStep::Quota;
                let config = GetConfigOperation::new(spec.auth_context.realm_id);
                Ok(Next::Call(IoCall::child(config)))
            }
            StagingStrategy::Sync => Err(ItemFailure::Stage(
                "sync is not a staging job strategy".to_string(),
            )),
        }
    }

    fn snapshot(&mut self, quota_ceiling: Option<u64>) -> Result<Next<u64>, ItemFailure> {
        let spec = self.run.spec.clone();
        let expected_bucket = self.bucket()?;
        self.step = StageStep::Write;
        let snapshot = MaterializeSnapshotOperation::new(
            MaterializeSnapshotInput {
                job_id: Some(self.run.job_id.as_ulid()),
                group_id: spec.group_id,
                user_id: spec.auth_context.user_id,
                realm_id: spec.auth_context.realm_id,
                node_id: spec.node_id,
                connector_id: spec.connector_id,
                source_path: self.source_path.clone(),
                bucket: spec.bucket.clone(),
                key: self.target_key.clone(),
                quota_ceiling,
                retry_key: Some(self.run.job_id.to_string()),
                expected_bucket,
                restrictions: spec.auth_context.path_restrictions.clone(),
                expected_checksums: std::mem::take(&mut self.expected),
            },
            self.run.routing.clone(),
        );
        Ok(Next::Call(IoCall::child(snapshot)))
    }

    fn replicate(&mut self, size: u64, version_id: ulid::Ulid) -> Next<u64> {
        let spec = &self.run.spec;
        self.step = StageStep::Replicate(size);
        Next::Call(IoCall::child(LiveVersionOperation::new(LiveVersionInput {
            local_node_id: spec.node_id,
            auth_context: spec.auth_context.clone(),
            bucket: spec.bucket.clone(),
            key: self.target_key.clone(),
            version_id,
            delete_marker: false,
        })))
    }
}

impl Operation for StageItemOperation {
    type Output = u64;
    type Error = ItemFailure;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<u64>, ItemFailure> {
        Ok(Next::Call(self.run.bucket_call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<u64>, ItemFailure> {
        let spec = self.run.spec.clone();
        match self.step {
            StageStep::Bucket => {
                self.bucket = Some(pinned(&spec, result)?);
                self.step = StageStep::Rules;
                Ok(Next::Call(rules_call(&spec)))
            }
            StageStep::Rules => {
                let rules = system::<PermissionRules, AuthorizationError>(result)?;
                file_access(&rules, &spec, &self.source_path, &self.target_key)?;
                self.mirrors_or_write()
            }
            StageStep::Mirrors => {
                self.mirrors = result.child::<_, ItemFailure>()??;
                self.write()
            }
            StageStep::Quota => {
                let config = system::<RealmConfigDocument, GetConfigError>(result)?;
                let ceiling = config.quota.effective_group_ceiling(&spec.group_id);
                self.snapshot(ceiling)
            }
            StageStep::Write => {
                let (size, version_id) = match spec.strategy {
                    StagingStrategy::Reference => {
                        let written = result
                            .child::<MaterializeReferenceResult, MaterializeReferenceError>()?
                            .map_err(|error| ItemFailure::Stage(error.to_string()))?;
                        (written.source_metadata.content_length, written.version_id)
                    }
                    _ => {
                        let written = result
                            .child::<MaterializeSnapshotResult, MaterializeSnapshotError>()?
                            .map_err(|error| ItemFailure::Stage(error.to_string()))?;
                        (written.location.blob_size, written.version_id)
                    }
                };
                Ok(self.replicate(size, version_id))
            }
            StageStep::Replicate(size) => {
                if let Err(error) = result.child::<QueueBlobResult, BlobQueueError>()? {
                    warn!(
                        bucket = %spec.bucket,
                        key = %self.target_key,
                        error = %error,
                        "Staging replication queue write failed"
                    );
                    return Err(ItemFailure::Stage(format!(
                        "replication queue failed: {error}"
                    )));
                }
                Ok(Next::Done(size))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ElsewhereStep {
    Bucket,
    Authority(Box<BucketInfo>),
}

/// Whether another coordinator of the distributed destination imports `key`.
#[derive(Debug, PartialEq)]
pub(super) struct ElsewhereOperation {
    run: StagingRun,
    key: String,
    step: ElsewhereStep,
}

impl ElsewhereOperation {
    pub(super) fn new(run: StagingRun, key: String) -> Self {
        Self {
            run,
            key,
            step: ElsewhereStep::Bucket,
        }
    }
}

impl Operation for ElsewhereOperation {
    type Output = bool;
    type Error = ItemFailure;

    fn first(&mut self) -> Result<Next<bool>, ItemFailure> {
        Ok(Next::Call(self.run.bucket_call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, ItemFailure> {
        match std::mem::replace(&mut self.step, ElsewhereStep::Bucket) {
            ElsewhereStep::Bucket => {
                let bucket = pinned(&self.run.spec, result)?;
                let Some(binding) = bucket.distribution.as_ref() else {
                    return Ok(Next::Done(false));
                };
                let call = authority_call(binding);
                self.step = ElsewhereStep::Authority(Box::new(bucket));
                Ok(Next::Call(call))
            }
            ElsewhereStep::Authority(bucket) => {
                let Some(binding) = bucket.distribution.as_ref() else {
                    return Ok(Next::Done(false));
                };
                let [record, fence] = values::<2>(result)?;
                let keys = [self.key.as_str()];
                let own = local_ranges(binding, &keys, record.as_deref(), fence.as_deref())
                    .map_err(ItemFailure::System)?;
                Ok(Next::Done(own.first() == Some(&None)))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum RunStep {
    Elsewhere,
    Stage,
    Moved(String),
}

/// One item of the spec's list, with the checkpoint it leaves. A key another coordinator
/// imports is skipped; one whose range moved after a failed write is left to that coordinator.
#[derive(Debug, PartialEq)]
pub(super) struct RunItemOperation {
    run: StagingRun,
    item: StagingJobItem,
    checkpoint: StagingJobCheckpoint,
    step: RunStep,
}

impl RunItemOperation {
    pub(super) fn new(
        run: StagingRun,
        item: StagingJobItem,
        mut checkpoint: StagingJobCheckpoint,
    ) -> Self {
        checkpoint.current_path = Some(item.source_path.clone());
        checkpoint.phase = item_phase(&run.spec);
        Self {
            run,
            item,
            checkpoint,
            step: RunStep::Elsewhere,
        }
    }

    fn finish(&mut self) -> Next<StagingJobCheckpoint> {
        let mut checkpoint = self.checkpoint.clone();
        checkpoint.items_done += 1;
        Next::Done(checkpoint)
    }

    fn stage(&mut self) -> Next<StagingJobCheckpoint> {
        self.step = RunStep::Stage;
        let paths = (self.item.source_path.clone(), self.item.target_key.clone());
        let index = self.checkpoint.items_done;
        let stage = StageItemOperation::new(self.run.clone(), paths, Vec::new(), index);
        Next::Call(IoCall::child(stage))
    }
}

impl Operation for RunItemOperation {
    type Output = StagingJobCheckpoint;
    type Error = PageFailure;

    fn first(&mut self) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        let key = self.item.target_key.clone();
        let elsewhere = ElsewhereOperation::new(self.run.clone(), key);
        Ok(Next::Call(IoCall::child(elsewhere)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StagingJobCheckpoint>, PageFailure> {
        match std::mem::replace(&mut self.step, RunStep::Stage) {
            RunStep::Elsewhere => match result.child::<bool, ItemFailure>()? {
                Ok(true) => Ok(self.finish()),
                Ok(false) => Ok(self.stage()),
                Err(ItemFailure::System(message)) => Err(PageFailure::Retry(message)),
                // A bucket that is gone or replaced fails the item below, as for any bucket.
                Err(_) => Ok(self.stage()),
            },
            RunStep::Stage => match result.child::<u64, ItemFailure>()? {
                Ok(size) => {
                    let checkpoint = &mut self.checkpoint;
                    checkpoint.items_current = checkpoint.items_current.saturating_add(1);
                    checkpoint.items_succeeded = checkpoint.items_succeeded.saturating_add(1);
                    if self.run.spec.strategy == StagingStrategy::Snapshot {
                        checkpoint.bytes_current = checkpoint.bytes_current.saturating_add(size);
                    }
                    Ok(self.finish())
                }
                // A key whose range moved meanwhile belongs to its new coordinator's job.
                Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => {
                    self.step = RunStep::Moved(message);
                    let key = self.item.target_key.clone();
                    let moved = ElsewhereOperation::new(self.run.clone(), key);
                    Ok(Next::Call(IoCall::child(moved)))
                }
                Err(ItemFailure::System(message)) => Err(PageFailure::Retry(message)),
                Err(ItemFailure::Lasting(message)) => Err(PageFailure::Permanent(message)),
            },
            RunStep::Moved(message) => {
                if !matches!(result.child::<bool, ItemFailure>()?, Ok(true)) {
                    let (source, target) = (&self.item.source_path, &self.item.target_key);
                    record_failure(&mut self.checkpoint, source, target, message);
                }
                Ok(self.finish())
            }
        }
    }
}

#[cfg(test)]
#[path = "item_tests.rs"]
mod tests;
