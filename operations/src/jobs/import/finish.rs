//! Finishes an import: rewrites its crate, creates or updates the dataset, and cleans up,
//! rolling the written objects back when the import failed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, HashMap, VecDeque};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::repository::RepositoryPull;
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::execution::job::{
    ImportReportDetail, ImportReportRow, ImportRoCrateSource, ReasonCode,
};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::secondary_id::RegisterIdentifiersSpec;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::data::identity::normalized_id;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::release::MembershipQuery;
use tracing::info;
use ulid::Ulid;

use super::plan::{federated_call, federated_result, metadata_permission_call};
use super::reports::{
    LoadReportsOperation, report_of, report_read, report_write, system_write, written,
};
use super::rewrite::{RewriteTarget, rewrite_document, validate_document};
use super::upload_store::{DeleteUploadOperation, UploadStoreError};
use super::write::{reference_kind_call, reference_kind_of};
use super::{
    ImportCheckpoint, ImportEntryPlan, ImportFailure, ImportPhase, ImportPlan, ImportRun,
    PINNED_KEPT, REMOVED, bucket_call, bucket_result, classify_metadata, ensure_valid_path,
    entry_arn, entry_location, entry_report, failure_message, hidden_delete, hidden_deleted,
    permission_result, rollback_required, transfer_failure, validation_failure,
};
use crate::forward::transport::MetadataWriteError;
use crate::jobs::export::CrateJsonOperation;
use crate::jobs::repository::links::LinkError;
use crate::jobs::repository::pull::{PullRunningOperation, SettleImportOperation};
use crate::jobs::repository::{TransferError, is_reference};
use crate::jobs::submit::{SubmitJobError, SubmitJobResult};
use crate::jobs::submit_local::SubmitLocalOperation;
use crate::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentOperation, CreateDocumentPayload, CreateDocumentResult,
};
use crate::metadata::forward::route_create::RouteCreateOperation;
use crate::metadata::forward::routed_update::{DocumentUpdate, RouteUpdateOperation};
use crate::metadata::record_reads::DocumentRecordOperation;
use crate::metadata::update_document::{UpdateDocumentError, UpdateDocumentMutation};
use crate::notifications::watch::origin::EmitEventOperation;
use crate::s3::bucket::release::BucketScope;
use crate::s3::object::delete::DeleteObjectError;
use crate::s3::object::delete::bulk::{
    BulkDeleteEntry, BulkDeleteInput, BulkDeleteOperation, BulkDeleteOutcome,
};
use crate::s3::object::delete::pinned::{PinnedError, ReleaseGuards, VersionPinsOperation};
use aruna_core::errors::StorageReadError;

fn actor_of(run: &ImportRun) -> Actor {
    let auth = &run.spec.auth_context;
    Actor {
        node_id: run.owner_node,
        user_id: auth.user_id,
        realm_id: auth.realm_id,
    }
}

fn rewritten_json(checkpoint: &ImportCheckpoint) -> Result<String, ImportFailure> {
    (checkpoint.rewritten_json.clone())
        .ok_or_else(|| ImportFailure::Permanent("rewritten RO-Crate is missing".to_string()))
}

#[derive(Debug, PartialEq)]
enum RewriteStep {
    Reports,
    Kind,
    Warnings,
}

/// Rewrites the crate's file identifiers to the imported objects.
#[derive(Debug, PartialEq)]
pub(super) struct RewriteCrateOperation {
    run: ImportRun,
    checkpoint: ImportCheckpoint,
    plan: ImportPlan,
    reports: HashMap<String, ImportReportRow>,
    warnings: VecDeque<ImportReportRow>,
    step: RewriteStep,
}

impl RewriteCrateOperation {
    pub(super) fn new(run: ImportRun, checkpoint: ImportCheckpoint, plan: ImportPlan) -> Self {
        Self {
            run,
            checkpoint,
            plan,
            reports: HashMap::new(),
            warnings: VecDeque::new(),
            step: RewriteStep::Reports,
        }
    }

    fn rewrite(
        &mut self,
        reference: Option<RepositoryConnectorKind>,
    ) -> Result<Next<ImportCheckpoint>, ImportFailure> {
        let spec = &self.run.spec;
        let validated = validate_document(&self.plan.metadata_json).map_err(validation_failure)?;
        let mut targets = HashMap::new();
        let mut used = BTreeSet::new();
        for entry in &self.plan.entries {
            let Some(file_id) = &entry.described_id else {
                continue;
            };
            let report = self.reports.get(&entry.path).ok_or_else(|| {
                ImportFailure::Permanent("import report row is missing".to_string())
            })?;
            let location = entry_location(spec, entry);
            // A reference has no content address yet, so its exact version names it.
            let id = if reference.is_some_and(|kind| is_reference(kind, &entry.path)) {
                entry_arn(spec, self.run.owner_node, entry)?.to_w3id()
            } else {
                let hash = (report.detail.blake3.as_deref())
                    .ok_or_else(|| ImportFailure::Permanent("imported hash is missing".into()))?;
                let hash: [u8; 32] = hex::decode(hash)
                    .ok()
                    .and_then(|hash| hash.try_into().ok())
                    .ok_or_else(|| ImportFailure::Permanent("imported hash is invalid".into()))?;
                normalized_id(hash, &location, &mut used)
            };
            targets.insert(
                file_id.clone(),
                RewriteTarget {
                    id,
                    content_url: location.to_url(),
                    local_path: entry.path.clone(),
                },
            );
        }
        let rewritten = rewrite_document(validated.value, &targets).map_err(validation_failure)?;
        self.warnings = (rewritten.warnings.iter().enumerate())
            .map(|(index, file_id)| ImportReportRow {
                entry_key: format!("warning/{index:08}"),
                code: ReasonCode::UnrewrittenReference,
                message: Some(format!(
                    "scalar reference to `{file_id}` could not be rewritten safely"
                )),
                detail: ImportReportDetail {
                    archive_path: file_id.clone(),
                    target_key: None,
                    version_id: None,
                    blake3: None,
                    size: None,
                    arn: None,
                    w3id: None,
                    validation: None,
                },
            })
            .collect();
        self.checkpoint.rewritten_json = Some(rewritten.jsonld);
        self.checkpoint.phase = ImportPhase::Create;
        self.step = RewriteStep::Warnings;
        self.next_warning()
    }

    fn next_warning(&mut self) -> Result<Next<ImportCheckpoint>, ImportFailure> {
        match self.warnings.pop_front() {
            Some(row) => Ok(Next::Call(system_write(&self.run, &row)?)),
            None => Ok(Next::Done(self.checkpoint.clone())),
        }
    }
}

impl Operation for RewriteCrateOperation {
    type Output = ImportCheckpoint;
    type Error = ImportFailure;

    fn first(&mut self) -> Result<Next<ImportCheckpoint>, ImportFailure> {
        // The document is checked before any row is read, as a broken one needs none of them.
        validate_document(&self.plan.metadata_json).map_err(validation_failure)?;
        let reports = LoadReportsOperation::new(self.run.clone());
        Ok(Next::Call(IoCall::child(reports)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ImportCheckpoint>, ImportFailure> {
        match self.step {
            RewriteStep::Reports => {
                self.reports =
                    result.child::<HashMap<String, ImportReportRow>, ImportFailure>()??;
                match reference_kind_call(&self.run) {
                    Some(call) => {
                        self.step = RewriteStep::Kind;
                        Ok(Next::Call(call))
                    }
                    None => self.rewrite(None),
                }
            }
            RewriteStep::Kind => {
                let kind = reference_kind_of(result)?;
                self.rewrite(Some(kind))
            }
            RewriteStep::Warnings => {
                written(result)?;
                self.next_warning()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum CreateStep {
    Allowed,
    Federated,
    Create,
    Emit,
}

/// Creates the imported dataset at a holder of its bucket and announces it.
#[derive(Debug, PartialEq)]
pub(super) struct CreateDatasetOperation {
    run: ImportRun,
    checkpoint: ImportCheckpoint,
    now_ms: u64,
    step: CreateStep,
}

impl CreateDatasetOperation {
    pub(super) fn new(run: ImportRun, checkpoint: ImportCheckpoint) -> Self {
        Self {
            run,
            checkpoint,
            now_ms: 0,
            step: CreateStep::Allowed,
        }
    }
}

impl Operation for CreateDatasetOperation {
    type Output = ImportCheckpoint;
    type Error = ImportFailure;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<ImportCheckpoint>, ImportFailure> {
        Ok(Next::Call(metadata_permission_call(&self.run)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ImportCheckpoint>, ImportFailure> {
        let spec = self.run.spec.clone();
        match self.step {
            CreateStep::Allowed => {
                permission_result(result)?;
                self.step = CreateStep::Federated;
                Ok(Next::Call(federated_call(&self.run, self.now_ms)))
            }
            CreateStep::Federated => {
                federated_result(result)?;
                ensure_valid_path(&spec)?;
                let jsonld = rewritten_json(&self.checkpoint)?;
                self.step = CreateStep::Create;
                Ok(Next::Call(IoCall::child(RouteCreateOperation::new(
                    CreateDocumentOperation::new_generated_id(CreateDocumentConfig {
                        actor: actor_of(&self.run),
                        group_id: spec.metadata.group_id,
                        document_id: spec.document_id,
                        document_path: spec.metadata.path.clone(),
                        public: spec.metadata.public,
                        payload: CreateDocumentPayload::RoCrate { jsonld },
                    }),
                    self.run.local_node,
                    Some(AuthToken::internal(spec.auth_context.clone())),
                ))))
            }
            CreateStep::Create => {
                let created = result.child::<CreateDocumentResult, MetadataWriteError>()?;
                let created = created.map_err(classify_metadata)?;
                let checkpoint = &mut self.checkpoint;
                checkpoint.created = true;
                checkpoint.phase = ImportPhase::Cleanup;
                if let Some(pull) = checkpoint.pull.as_mut() {
                    pull.revision = Some(created.event_id);
                }
                self.step = CreateStep::Emit;
                let auth = &spec.auth_context;
                Ok(Next::Call(IoCall::child(
                    EmitEventOperation::metadata_created(
                        auth.realm_id,
                        auth.user_id,
                        created.record.group_id,
                        created.record.document_id,
                        &created.record.document_path,
                        created.event_id,
                    ),
                )))
            }
            // Failures only warn inside the operation.
            CreateStep::Emit => Ok(Next::Done(self.checkpoint.clone())),
        }
    }
}

#[derive(Debug, PartialEq)]
enum UpdateStep {
    Running,
    Allowed,
    Current,
    Record,
    Update,
}

/// Replaces the linked dataset's crate with the merged one through the normal update, unless the
/// dataset changed since the pull read it. A retry finds its own update by the crate it wrote.
#[derive(Debug, PartialEq)]
pub(super) struct UpdateDatasetOperation {
    run: ImportRun,
    checkpoint: ImportCheckpoint,
    base: Option<Ulid>,
    step: UpdateStep,
}

impl UpdateDatasetOperation {
    pub(super) fn new(run: ImportRun, checkpoint: ImportCheckpoint) -> Self {
        Self {
            run,
            checkpoint,
            base: None,
            step: UpdateStep::Running,
        }
    }

    fn finish(&mut self, revision: Ulid) -> Next<ImportCheckpoint> {
        let checkpoint = &mut self.checkpoint;
        if let Some(pull) = checkpoint.pull.as_mut() {
            pull.revision = Some(revision);
        }
        checkpoint.created = true;
        checkpoint.phase = ImportPhase::Cleanup;
        Next::Done(checkpoint.clone())
    }

    fn check_allowed(&mut self) -> Next<ImportCheckpoint> {
        self.step = UpdateStep::Allowed;
        Next::Call(metadata_permission_call(&self.run))
    }
}

impl Operation for UpdateDatasetOperation {
    type Output = ImportCheckpoint;
    type Error = ImportFailure;

    fn first(&mut self) -> Result<Next<ImportCheckpoint>, ImportFailure> {
        let spec = &self.run.spec;
        // Pausing or deleting the link while the files were imported cancels the update.
        if let ImportRoCrateSource::Repository {
            pull: Some(RepositoryPull::Update { link_id }),
            ..
        } = &spec.source
        {
            let running = PullRunningOperation::new(self.run.job_id, spec.document_id, *link_id);
            return Ok(Next::Call(IoCall::child(running)));
        }
        Ok(self.check_allowed())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ImportCheckpoint>, ImportFailure> {
        let spec = self.run.spec.clone();
        match self.step {
            UpdateStep::Running => {
                (result.child::<(), TransferError>()?).map_err(transfer_failure)?;
                Ok(self.check_allowed())
            }
            UpdateStep::Allowed => {
                permission_result(result)?;
                rewritten_json(&self.checkpoint)?;
                let base = (self.checkpoint.pull.as_ref())
                    .and_then(|pull| pull.base)
                    .ok_or_else(|| {
                        ImportFailure::Permanent("pull base revision is missing".to_string())
                    })?;
                self.base = Some(base);
                self.step = UpdateStep::Current;
                Ok(Next::Call(IoCall::child(CrateJsonOperation::new(
                    self.run.local_node,
                    spec.auth_context.clone(),
                    spec.document_id,
                    spec.limits.metadata_bytes,
                ))))
            }
            UpdateStep::Current => {
                let (current, event_id) =
                    (result.child::<(String, Ulid), TransferError>()?).map_err(transfer_failure)?;
                if Some(event_id) == self.base {
                    self.step = UpdateStep::Record;
                    let record = DocumentRecordOperation::new(spec.document_id);
                    return Ok(Next::Call(IoCall::child(record)));
                }
                // The update landed when the dataset holds exactly the crate this pull wrote.
                let jsonld = rewritten_json(&self.checkpoint)?;
                let digest = |jsonld: &str| {
                    craqle::canonicalize_jsonld(jsonld)
                        .ok()
                        .map(|value| value.digest)
                };
                if digest(&current).is_none() || digest(&current) != digest(&jsonld) {
                    return Err(ImportFailure::Permanent(
                        "the dataset changed while the pull ran; pull again".to_string(),
                    ));
                }
                Ok(self.finish(event_id))
            }
            UpdateStep::Record => {
                let record = (result
                    .child::<Option<MetadataRegistryRecord>, StorageReadError>()?)
                .map_err(|error| ImportFailure::Retryable(error.to_string()))?;
                let jsonld = rewritten_json(&self.checkpoint)?;
                self.step = UpdateStep::Update;
                // The holder refuses the update inside its transaction when an edit landed
                // after the base.
                Ok(Next::Call(IoCall::child(RouteUpdateOperation::new(
                    (actor_of(&self.run), self.run.local_node),
                    (record, spec.document_id),
                    DocumentUpdate {
                        public: None,
                        mutation: UpdateDocumentMutation::ReplaceRoCrate { jsonld },
                        expected_revision: self.base,
                        commit_message: None,
                    },
                    Some(AuthToken::internal(spec.auth_context.clone())),
                ))))
            }
            UpdateStep::Update => {
                match result.child::<MetadataRegistryRecord, MetadataWriteError>()? {
                    Ok(updated) => Ok(self.finish(updated.last_event_id)),
                    Err(MetadataWriteError::Update(UpdateDocumentError::RevisionConflict {
                        ..
                    })) => Err(ImportFailure::Permanent(
                        "the dataset changed while the pull updated it; check it and pull again"
                            .to_string(),
                    )),
                    Err(error) => Err(classify_metadata(error)),
                }
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum RollbackStep {
    Bucket,
    Pins(BucketInfo),
    KeptRead,
    KeptWrite,
    Delete,
    Missing,
    Removed,
    RemovedWrite,
}

/// Deletes exactly the object versions this import wrote, but keeps and returns those a release
/// pins. Every version id is minted by this job, so versions another writer owns are never
/// removed, even where the import landed on an existing key.
#[derive(Debug, PartialEq)]
pub(super) struct RollbackOperation {
    run: ImportRun,
    attempted: Vec<ImportEntryPlan>,
    pinned: VecDeque<ImportEntryPlan>,
    removable: Vec<ImportEntryPlan>,
    bucket: Option<BucketInfo>,
    guards: ReleaseGuards,
    outcomes: VecDeque<(ImportEntryPlan, BulkDeleteOutcome)>,
    current: Option<ImportEntryPlan>,
    kept: Vec<String>,
    removed: u64,
    step: RollbackStep,
}

/// What a rollback removed and which keys a release kept.
pub(super) type RollbackEnd = (u64, Vec<String>);

impl RollbackOperation {
    pub(super) fn new(run: ImportRun, plan: &ImportPlan, next_entry: usize) -> Self {
        // The entry at `next_entry` may have landed before the failure was seen.
        let attempted = plan.entries.len().min(next_entry.saturating_add(1));
        Self {
            run,
            attempted: plan.entries[..attempted].to_vec(),
            pinned: VecDeque::new(),
            removable: Vec::new(),
            bucket: None,
            guards: Vec::new(),
            outcomes: VecDeque::new(),
            current: None,
            kept: Vec::new(),
            removed: 0,
            step: RollbackStep::Bucket,
        }
    }

    fn next_kept(&mut self) -> Result<Next<RollbackEnd>, ImportFailure> {
        match self.pinned.front() {
            Some(entry) => {
                self.step = RollbackStep::KeptRead;
                Ok(Next::Call(report_read(&self.run, &entry.path)))
            }
            None => self.delete(),
        }
    }

    fn delete(&mut self) -> Result<Next<RollbackEnd>, ImportFailure> {
        let spec = &self.run.spec;
        let auth = &spec.auth_context;
        let bucket = self.bucket.as_ref().ok_or_else(lost_rollback)?;
        let entries = (self.removable.iter())
            .map(|entry| BulkDeleteEntry {
                key: entry.target_key.clone(),
                version_id: Some(entry.version_id),
            })
            .collect();
        self.step = RollbackStep::Delete;
        Ok(Next::Call(IoCall::child(BulkDeleteOperation::new(
            BulkDeleteInput {
                bucket: spec.target.bucket.clone(),
                entries,
                group_id: bucket.group_id,
                realm_id: auth.realm_id,
                node_id: self.run.owner_node,
                deleted_by: auth.user_id,
                restrictions: auth.path_restrictions.clone(),
                guards: std::mem::take(&mut self.guards),
            },
        ))))
    }

    /// Goes through the delete outcomes; only a version that was really removed loses its
    /// report detail, so the entry that failed before writing keeps its own failure message.
    fn next_outcome(&mut self) -> Result<Next<RollbackEnd>, ImportFailure> {
        let Some((entry, outcome)) = self.outcomes.pop_front() else {
            info!(
                job_id = %self.run.job_id,
                removed = self.removed,
                "Rolled back objects written by a failed RO-Crate import"
            );
            return Ok(Next::Done((self.removed, std::mem::take(&mut self.kept))));
        };
        let read = report_read(&self.run, &entry.path);
        self.current = Some(entry);
        match outcome.result {
            Ok(_) => {
                self.removed += 1;
                self.step = RollbackStep::Removed;
                Ok(Next::Call(read))
            }
            // An earlier attempt may have removed it; its report row still counts it.
            Err(DeleteObjectError::NoSuchVersion) => {
                self.step = RollbackStep::Missing;
                Ok(Next::Call(read))
            }
            Err(error) => Err(ImportFailure::Retryable(format!(
                "rollback of `{}` failed: {error}",
                outcome.key
            ))),
        }
    }
}

fn lost_rollback() -> ImportFailure {
    ImportFailure::Retryable("the rollback lost its state".to_string())
}

impl Operation for RollbackOperation {
    type Output = RollbackEnd;
    type Error = ImportFailure;

    fn first(&mut self) -> Result<Next<RollbackEnd>, ImportFailure> {
        if self.attempted.is_empty() {
            return Ok(Next::Done((0, Vec::new())));
        }
        Ok(Next::Call(bucket_call(&self.run.spec.target.bucket)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RollbackEnd>, ImportFailure> {
        let spec = self.run.spec.clone();
        match std::mem::replace(&mut self.step, RollbackStep::Removed) {
            RollbackStep::Bucket => {
                // An unreachable bucket leaves the rollback unfinished, so it must requeue
                // instead of reporting a clean one.
                let bucket = bucket_result(&spec.target.bucket, result).map_err(|error| {
                    ImportFailure::Retryable(format!(
                        "rollback cannot read bucket `{}`: {}",
                        spec.target.bucket,
                        failure_message(error)
                    ))
                })?;
                let scope = BucketScope {
                    realm_id: spec.auth_context.realm_id,
                    node_id: self.run.owner_node,
                    bucket: &spec.target.bucket,
                    info: &bucket,
                };
                let queries: Vec<MembershipQuery> = (self.attempted.iter())
                    .map(|entry| MembershipQuery {
                        key: entry.target_key.clone(),
                        version_id: entry.version_id,
                    })
                    .collect();
                let pins = VersionPinsOperation::new(scope, &spec.auth_context, queries);
                self.step = RollbackStep::Pins(bucket);
                Ok(Next::Call(IoCall::child(pins)))
            }
            RollbackStep::Pins(bucket) => {
                let pins = result.child::<(Vec<bool>, ReleaseGuards), PinnedError>()?;
                let (pins, guards) = pins.map_err(|error| {
                    ImportFailure::Retryable(format!("rollback cannot check releases: {error}"))
                })?;
                self.bucket = Some(bucket);
                self.guards = guards;
                for (entry, pinned) in std::mem::take(&mut self.attempted).into_iter().zip(pins) {
                    match pinned {
                        true => self.pinned.push_back(entry),
                        false => self.removable.push(entry),
                    }
                }
                self.next_kept()
            }
            RollbackStep::KeptRead => {
                let entry = self.pinned.pop_front().ok_or_else(lost_rollback)?;
                let mut row = report_of(result)?.unwrap_or_else(|| entry_report(&entry));
                row.code = ReasonCode::Failed;
                row.message = Some(
                    "written object was kept after the import failed: a release pins it"
                        .to_string(),
                );
                self.kept.push(entry.target_key);
                self.step = RollbackStep::KeptWrite;
                Ok(Next::Call(report_write(&self.run, &row)?))
            }
            RollbackStep::KeptWrite => {
                written(result)?;
                self.next_kept()
            }
            RollbackStep::Delete => {
                let outcomes = result.child::<Vec<BulkDeleteOutcome>, DeleteObjectError>()?;
                let outcomes = outcomes.map_err(|error| {
                    ImportFailure::Retryable(format!("rollback delete failed: {error}"))
                })?;
                let removable = std::mem::take(&mut self.removable);
                self.outcomes = removable.into_iter().zip(outcomes).collect();
                self.next_outcome()
            }
            RollbackStep::Missing => {
                let row = report_of(result)?;
                if row.is_some_and(|row| row.message.as_deref() == Some(REMOVED)) {
                    self.removed += 1;
                }
                self.next_outcome()
            }
            RollbackStep::Removed => {
                let entry = self.current.take().ok_or_else(lost_rollback)?;
                let mut row = report_of(result)?.unwrap_or_else(|| entry_report(&entry));
                row.code = ReasonCode::Failed;
                row.message = Some(REMOVED.to_string());
                row.detail.version_id = None;
                row.detail.blake3 = None;
                row.detail.size = None;
                row.detail.arn = None;
                row.detail.w3id = None;
                self.step = RollbackStep::RemovedWrite;
                Ok(Next::Call(report_write(&self.run, &row)?))
            }
            RollbackStep::RemovedWrite => {
                written(result)?;
                self.next_outcome()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum CleanupStep {
    Rollback,
    Hidden,
    Upload,
    Identifiers,
    Settle,
}

/// Rolls a failed import back, drops its hidden source, queues the source repository's
/// identifiers for the created dataset and settles a pull link.
#[derive(Debug, PartialEq)]
pub(super) struct CleanupOperation {
    run: ImportRun,
    checkpoint: ImportCheckpoint,
    plan: Option<ImportPlan>,
    now_ms: u64,
    step: CleanupStep,
}

type CleanupTurn = Result<Next<ImportCheckpoint>, ImportFailure>;

impl CleanupOperation {
    pub(super) fn new(
        run: ImportRun,
        checkpoint: ImportCheckpoint,
        plan: Option<ImportPlan>,
    ) -> Self {
        Self {
            run,
            checkpoint,
            plan,
            now_ms: 0,
            step: CleanupStep::Hidden,
        }
    }

    fn drop_hidden(&mut self) -> CleanupTurn {
        let Some(input) = self.checkpoint.input.as_ref() else {
            return self.identifiers();
        };
        let delete = hidden_delete(&input.location).map_err(ImportFailure::Retryable)?;
        self.step = CleanupStep::Hidden;
        Ok(Next::Call(delete))
    }

    fn identifiers(&mut self) -> CleanupTurn {
        self.checkpoint.refs.hidden_locations.clear();
        if !self.checkpoint.created || self.checkpoint.identifiers.is_empty() {
            return self.settle();
        }
        let spec = &self.run.spec;
        // The queued job waits for the PID authority, so cleanup never does.
        let submit = SubmitLocalOperation::identifiers(
            RegisterIdentifiersSpec {
                document_id: spec.document_id,
                identifiers: self.checkpoint.identifiers.clone(),
                auth_context: spec.auth_context.clone(),
            },
            self.run.owner_node,
            format!("identifiers/{}", self.run.job_id),
            self.run.scope.node,
            self.now_ms,
        );
        self.step = CleanupStep::Identifiers;
        Ok(Next::Call(IoCall::child(submit)))
    }

    fn settle(&mut self) -> CleanupTurn {
        self.step = CleanupStep::Settle;
        Ok(Next::Call(IoCall::child(SettleImportOperation::new(
            self.run.scope.clone(),
            self.run.job_id,
            self.run.owner_node,
            (*self.run.spec).clone(),
            self.checkpoint.pull.clone(),
            self.checkpoint.failure.clone(),
        ))))
    }
}

impl Operation for CleanupOperation {
    type Output = ImportCheckpoint;
    type Error = ImportFailure;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> CleanupTurn {
        match self
            .plan
            .as_ref()
            .filter(|_| rollback_required(&self.checkpoint))
        {
            Some(plan) => {
                self.step = CleanupStep::Rollback;
                let next_entry = self.checkpoint.next_entry;
                let rollback = RollbackOperation::new(self.run.clone(), plan, next_entry);
                Ok(Next::Call(IoCall::child(rollback)))
            }
            None => self.drop_hidden(),
        }
    }

    fn next(&mut self, result: IoResult) -> CleanupTurn {
        match self.step {
            CleanupStep::Rollback => {
                let (removed, kept) = result.child::<RollbackEnd, ImportFailure>()??;
                let checkpoint = &mut self.checkpoint;
                checkpoint.rolled_back = removed;
                if let Some(failure) = checkpoint.failure.as_mut().filter(|_| !kept.is_empty()) {
                    failure.push_str(&format!("; {PINNED_KEPT}: `{}`", kept.join("`, `")));
                }
                self.drop_hidden()
            }
            CleanupStep::Hidden => {
                hidden_deleted(result).map_err(ImportFailure::Retryable)?;
                let upload_id = self.checkpoint.input.as_ref().and_then(|it| it.upload_id);
                let Some(upload_id) = upload_id else {
                    return self.identifiers();
                };
                self.step = CleanupStep::Upload;
                let delete = DeleteUploadOperation::new(upload_id, self.run.job_id);
                Ok(Next::Call(IoCall::child(delete)))
            }
            CleanupStep::Upload => {
                (result.child::<(), UploadStoreError>()?)
                    .map_err(|error| ImportFailure::Retryable(error.0))?;
                self.identifiers()
            }
            CleanupStep::Identifiers => {
                (result.child::<SubmitJobResult, SubmitJobError>()?).map_err(|error| {
                    ImportFailure::Retryable(format!(
                        "queueing repository identifiers failed: {error}"
                    ))
                })?;
                self.checkpoint.identifiers.clear();
                self.settle()
            }
            CleanupStep::Settle => {
                (result.child::<(), LinkError>()?).map_err(|error| {
                    ImportFailure::Retryable(format!("recording the pull link failed: {error}"))
                })?;
                self.checkpoint.phase = ImportPhase::Done;
                Ok(Next::Done(self.checkpoint.clone()))
            }
        }
    }
}

#[cfg(test)]
#[path = "finish_tests.rs"]
mod tests;
