//! Writes one planned archive entry as an object version, or as a repository reference, and
//! records it in the job's report.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::jobs::{ArchiveEntryRead, ByteStream, JobEffect, JobEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::execution::job::ImportReportRow;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use ulid::Ulid;

use super::plan::{federated_call, federated_result};
use super::reports::{report_of, report_read, report_write, system_write, written};
use super::{
    ImportCheckpoint, ImportEntryPlan, ImportFailure, ImportPhase, ImportPlan, ImportRun,
    bucket_call, bucket_result, classify_gate, classify_put, entry_arn, entry_report, job_call,
    permission_call, permission_result, stopped_failure, stops_of, transfer_failure, unexpected,
};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::groups::routing::{BucketRules, RoutingInputsError, SnapshotOperation};
use crate::harvest::read_connector::ConnectorView;
use crate::jobs::repository::connector::ConnectorViewOperation;
use crate::jobs::repository::invenio::reference::WriteRepositoryReference;
use crate::jobs::repository::{TransferError, is_reference, reference_source};
use crate::placement::policy::{GateContext, GateContextError, GateOperation};
use crate::replication::queue::{
    BlobQueueError, LiveVersionInput, LiveVersionOperation, QueueBlobResult,
};
use crate::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, PutObjectResult,
};

/// The connector kind of a repository import in reference mode, read through its connector.
pub(super) fn reference_kind_call(run: &ImportRun) -> Option<IoCall> {
    let (group_id, connector_id) = reference_source(&run.spec)?;
    let view = ConnectorViewOperation::new(group_id, connector_id);
    Some(IoCall::child(view))
}

/// The answer to [`reference_kind_call`].
pub(super) fn reference_kind_of(
    result: IoResult,
) -> Result<RepositoryConnectorKind, ImportFailure> {
    let view = (result.child::<ConnectorView, TransferError>()?).map_err(transfer_failure)?;
    Ok(view.connector.kind)
}

#[derive(Debug, PartialEq)]
enum Step {
    Federated,
    Bucket,
    Allowed,
    Report,
    Version,
    Open,
    Guard,
    Kind,
    Descriptor(RepositoryConnectorKind),
    Reference,
    ReferenceReport,
    ReferenceQueue,
    Quota,
    Routing(Option<u64>),
    Gate(Option<u64>, Box<RoutingSnapshot>),
    Put,
    PutStopped(PutObjectError),
    Divergent,
    FinalReport,
    FinalQueue,
}

/// Imports the entry at the checkpoint's position; past the last entry the phase moves on.
#[derive(Debug, PartialEq)]
pub(super) struct WriteEntryOperation {
    run: ImportRun,
    checkpoint: ImportCheckpoint,
    entry: Option<ImportEntryPlan>,
    bucket: Option<BucketInfo>,
    row: Option<ImportReportRow>,
    body: Option<ByteStream>,
    now_ms: u64,
    index: u64,
    step: Step,
}

type Turn = Result<Next<ImportCheckpoint>, ImportFailure>;

impl WriteEntryOperation {
    pub(super) fn new(run: ImportRun, checkpoint: ImportCheckpoint, plan: &ImportPlan) -> Self {
        let entry = plan.entries.get(checkpoint.next_entry).cloned();
        let index = checkpoint.next_entry as u64;
        Self {
            run,
            checkpoint,
            entry,
            bucket: None,
            row: None,
            body: None,
            now_ms: 0,
            index,
            step: Step::Federated,
        }
    }

    fn entry(&self) -> Result<&ImportEntryPlan, ImportFailure> {
        self.entry.as_ref().ok_or_else(lost)
    }

    fn row(&mut self) -> Result<&mut ImportReportRow, ImportFailure> {
        self.row.as_mut().ok_or_else(lost)
    }

    fn open_entry(&mut self) -> Result<Next<ImportCheckpoint>, ImportFailure> {
        let entry = self.entry()?;
        let input = (self.checkpoint.input.as_ref())
            .ok_or_else(|| ImportFailure::Permanent("import source is missing".to_string()))?;
        let read = ArchiveEntryRead {
            location: input.location.clone(),
            size: input.size,
            data_offset: entry.data_offset,
            compressed_size: entry.compressed_size,
            uncompressed_size: entry.uncompressed_size,
            compression: entry.compression,
        };
        self.step = Step::Open;
        Ok(Next::Call(job_call(JobEffect::OpenArchiveEntry(Box::new(
            read,
        )))))
    }

    /// Reads the connector kind of a reference import; other imports go on at once.
    fn check_kind(&mut self) -> Turn {
        match reference_kind_call(&self.run) {
            Some(call) => {
                self.step = Step::Kind;
                Ok(Next::Call(call))
            }
            None => self.quota(),
        }
    }

    fn quota(&mut self) -> Turn {
        self.step = Step::Quota;
        let realm_id = self.run.spec.auth_context.realm_id;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn put(
        &mut self,
        quota_ceiling: Option<u64>,
        routing: RoutingSnapshot,
        gate: Option<GateContext>,
    ) -> Turn {
        let spec = self.run.spec.clone();
        let entry = self.entry()?.clone();
        let bucket = self.bucket.take().ok_or_else(lost)?;
        let body = self.body.take().ok_or_else(lost)?;
        let mut operation = PutObjectOperation::new(PutObjectConfig {
            user_id: spec.auth_context.user_id,
            group_id: bucket.group_id,
            realm_id: spec.auth_context.realm_id,
            node_id: self.run.owner_node,
            request: PutObjectInput {
                bucket: spec.target.bucket.clone(),
                key: entry.target_key.clone(),
                content_length: Some(entry.uncompressed_size),
                body: Some(body),
            },
            expected_checksums: vec![ExpectedChecksum {
                algorithm: ChecksumAlgorithm::Crc32,
                digest: entry.crc32.to_be_bytes().to_vec(),
            }],
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: Some(entry.version_id),
            quota_ceiling,
            routing,
        })
        .with_bucket_guard(bucket)
        .create_only()
        .with_rocrate_limits(spec.limits.clone())
        .with_restrictions(spec.auth_context.path_restrictions.clone())
        .with_version_origin(Some(VersionOrigin::Source {
            job_id: Some(self.run.job_id.as_ulid()),
        }));
        if let Some(gate) = gate {
            operation = operation.with_gate(gate);
        }
        self.step = Step::Put;
        Ok(Next::Call(IoCall::child(operation)))
    }

    fn put_done(&mut self, put: PutObjectResult) -> Turn {
        let entry = self.entry()?.clone();
        // The preflight validated identifiers built from the planned version.
        if put.version_id != entry.version_id {
            let mut row = entry_report(&entry);
            row.entry_key = format!("divergent/{}", entry.path);
            row.code = aruna_core::structs::execution::job::ReasonCode::Failed;
            row.message = Some(format!(
                "object write returned version {} instead of the planned {}; \
                 the written version was left in place",
                put.version_id, entry.version_id
            ));
            row.detail.version_id = Some(put.version_id);
            self.step = Step::Divergent;
            return Ok(Next::Call(system_write(&self.run, &row)?));
        }
        let hash: [u8; 32] = (put.location.get_blake3())
            .and_then(|hash| hash.try_into().ok())
            .ok_or_else(|| ImportFailure::Retryable("written object has no BLAKE3 hash".into()))?;
        let arn = entry_arn(&self.run.spec, self.run.owner_node, &entry)?;
        let row = self.row()?;
        row.detail.version_id = Some(put.version_id);
        row.detail.blake3 = Some(hex::encode(hash));
        row.detail.size = Some(put.location.blob_size);
        row.detail.arn = Some(arn.to_string());
        row.detail.w3id = Some(arn.to_w3id());
        let row = row.clone();
        self.step = Step::FinalReport;
        Ok(Next::Call(report_write(&self.run, &row)?))
    }

    fn queue(&self) -> Result<IoCall, ImportFailure> {
        let spec = &self.run.spec;
        let entry = self.entry()?;
        Ok(IoCall::child(LiveVersionOperation::new(LiveVersionInput {
            local_node_id: self.run.owner_node,
            auth_context: spec.auth_context.clone(),
            bucket: spec.target.bucket.clone(),
            key: entry.target_key.clone(),
            version_id: entry.version_id,
            delete_marker: false,
        })))
    }

    fn advanced(&mut self) -> Next<ImportCheckpoint> {
        self.checkpoint.next_entry = self.checkpoint.next_entry.saturating_add(1);
        Next::Done(self.checkpoint.clone())
    }
}

fn lost() -> ImportFailure {
    ImportFailure::Retryable("the import entry lost its state".to_string())
}

impl Operation for WriteEntryOperation {
    type Output = ImportCheckpoint;
    type Error = ImportFailure;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn {
        Ok(Next::Call(federated_call(&self.run, self.now_ms)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        let spec = self.run.spec.clone();
        let auth = &spec.auth_context;
        match std::mem::replace(&mut self.step, Step::FinalQueue) {
            Step::Federated => {
                federated_result(result)?;
                if self.entry.is_none() {
                    self.checkpoint.phase = ImportPhase::Rewrite;
                    return Ok(Next::Done(self.checkpoint.clone()));
                }
                self.step = Step::Bucket;
                Ok(Next::Call(bucket_call(&spec.target.bucket)))
            }
            Step::Bucket => {
                let bucket = bucket_result(&spec.target.bucket, result)?;
                let path = object_permission_path(
                    auth.realm_id,
                    bucket.group_id,
                    self.run.owner_node,
                    &spec.target.bucket,
                    &self.entry()?.target_key,
                );
                self.bucket = Some(bucket);
                self.step = Step::Allowed;
                Ok(Next::Call(permission_call(auth, path, Permission::WRITE)))
            }
            Step::Allowed => {
                permission_result(result)?;
                self.step = Step::Report;
                Ok(Next::Call(report_read(&self.run, &self.entry()?.path)))
            }
            Step::Report => {
                let entry = self.entry()?.clone();
                let mut row = report_of(result)?.unwrap_or_else(|| entry_report(&entry));
                if row.detail.version_id.is_some() {
                    self.row = Some(row);
                    return self.open_entry();
                }
                row.detail.version_id = Some(entry.version_id);
                let write = report_write(&self.run, &row)?;
                self.row = Some(row);
                self.step = Step::Version;
                Ok(Next::Call(write))
            }
            Step::Version => {
                written(result)?;
                self.open_entry()
            }
            Step::Open => match result.event()? {
                Event::Job(JobEvent::ArchiveEntry(stream)) => {
                    let stream = stream.map_err(ImportFailure::Permanent)?;
                    self.step = Step::Guard;
                    Ok(Next::Call(job_call(JobEffect::StoppableStream(stream))))
                }
                other => Err(unexpected("the opened archive entry", &other).into()),
            },
            Step::Guard => match result.event()? {
                Event::Job(JobEvent::Stream(stream)) => {
                    self.body = Some(stream);
                    self.check_kind()
                }
                other => Err(unexpected("the guarded stream", &other).into()),
            },
            Step::Kind => {
                let kind = reference_kind_of(result)?;
                if !is_reference(kind, &self.entry()?.path) {
                    return self.quota();
                }
                let stream = self.body.take().ok_or_else(lost)?;
                let limit = spec.limits.metadata_bytes;
                self.step = Step::Descriptor(kind);
                Ok(Next::Call(job_call(JobEffect::CollectUpTo {
                    stream,
                    limit,
                })))
            }
            Step::Descriptor(kind) => {
                let event = result.event()?;
                let Event::Job(JobEvent::CollectedUpTo(bytes)) = event else {
                    return Err(unexpected("the reference descriptor", &event).into());
                };
                let bytes = bytes.map_err(ImportFailure::Retryable)?;
                let bytes = bytes.ok_or_else(|| {
                    ImportFailure::Permanent("reference descriptor exceeds limit".into())
                })?;
                let descriptor = serde_json::from_slice(&bytes)
                    .map_err(|_| ImportFailure::Permanent("invalid reference descriptor".into()))?;
                let entry = self.entry()?.clone();
                let bucket = self.bucket.take().ok_or_else(lost)?;
                let random = (u128::from(self.run.seed) << 32) | u128::from(self.index);
                self.step = Step::Reference;
                Ok(Next::Call(IoCall::child(WriteRepositoryReference::new(
                    kind,
                    self.run.scope.clone(),
                    self.run.job.clone(),
                    (*spec).clone(),
                    bucket,
                    entry.target_key,
                    entry.version_id,
                    descriptor,
                    Ulid::from_parts(self.now_ms, random),
                ))))
            }
            Step::Reference => {
                let metadata =
                    (result.child::<SourceMetadata, TransferError>()?).map_err(transfer_failure)?;
                let entry = self.entry()?.clone();
                let arn = entry_arn(&spec, self.run.owner_node, &entry)?;
                let row = self.row()?;
                row.detail.size = Some(metadata.content_length);
                row.detail.arn = Some(arn.to_string());
                row.detail.w3id = Some(arn.to_w3id());
                let row = row.clone();
                let write = report_write(&self.run, &row)?;
                self.step = Step::ReferenceReport;
                Ok(Next::Call(write))
            }
            Step::ReferenceReport => {
                written(result)?;
                self.step = Step::ReferenceQueue;
                Ok(Next::Call(self.queue()?))
            }
            Step::ReferenceQueue => {
                (result.child::<QueueBlobResult, BlobQueueError>()?)
                    .map_err(|error| ImportFailure::Retryable(error.to_string()))?;
                self.checkpoint.imported = self.checkpoint.imported.saturating_add(1);
                Ok(self.advanced())
            }
            Step::Quota => {
                let config = (result.child::<RealmConfigDocument, GetConfigError>()?)
                    .map_err(|error| ImportFailure::Retryable(error.to_string()))?;
                let bucket = self.bucket.as_ref().ok_or_else(lost)?;
                let quota = config.quota.effective_group_ceiling(&bucket.group_id);
                self.step = Step::Routing(quota);
                Ok(Next::Call(IoCall::child(SnapshotOperation::new(
                    &self.run.routing,
                    bucket.group_id,
                    BucketRules::of(bucket),
                ))))
            }
            Step::Routing(quota) => {
                let routing = result.child::<RoutingSnapshot, RoutingInputsError>()?;
                let routing = routing.map_err(|error| match error.storage() {
                    Some(_) => ImportFailure::Retryable(error.to_string()),
                    None => ImportFailure::Permanent(error.to_string()),
                })?;
                self.step = Step::Gate(quota, Box::new(routing));
                // The archive entry is external, so it carries no refs of its own: only
                // the destination default governs the imported object.
                let gate = GateOperation::new(auth.realm_id, self.now_ms);
                Ok(Next::Call(IoCall::child(gate)))
            }
            Step::Gate(quota, routing) => {
                let gate = (result.child::<Option<GateContext>, GateContextError>()?)
                    .map_err(classify_gate)?;
                // A node that stopped admitting governed data imports nothing until the
                // transition it is in ends; the import resumes from its checkpoint.
                if gate.as_ref().is_some_and(|gate| !gate.admitting) {
                    return Err(ImportFailure::Retryable(
                        "this node is not admitting governed data right now".to_string(),
                    ));
                }
                self.put(quota, *routing, gate)
            }
            Step::Put => match result.child::<PutObjectResult, PutObjectError>()? {
                Ok(put) => self.put_done(put),
                Err(error) => {
                    self.step = Step::PutStopped(error);
                    Ok(Next::Call(job_call(JobEffect::Stops)))
                }
            },
            Step::PutStopped(error) => {
                Err(stopped_failure(stops_of(result)?).unwrap_or_else(|| classify_put(error)))
            }
            Step::Divergent => {
                written(result)?;
                Err(ImportFailure::Permanent(
                    "written object version does not match the planned version".to_string(),
                ))
            }
            Step::FinalReport => {
                written(result)?;
                let code = self.entry()?.code;
                let checkpoint = &mut self.checkpoint;
                use aruna_core::structs::execution::job::ReasonCode;
                match code {
                    ReasonCode::Imported => {
                        checkpoint.imported = checkpoint.imported.saturating_add(1)
                    }
                    ReasonCode::Unlisted => {
                        checkpoint.unlisted = checkpoint.unlisted.saturating_add(1)
                    }
                    _ => {}
                }
                self.step = Step::FinalQueue;
                Ok(Next::Call(self.queue()?))
            }
            // A failed queue write leaves the obligation for repair; the import goes on.
            Step::FinalQueue => Ok(self.advanced()),
        }
    }
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
