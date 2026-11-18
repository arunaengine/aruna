//! Inspects the stored archive and plans the import: which entry goes to which key, under which
//! version, checked against the crate before anything is written.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::jobs::{ArchiveRead, JobEffect, JobEvent};
use aruna_core::operation::Operation;
use aruna_core::rocrate_archive::{
    ArchiveInspection, file_id_candidates, payload_entries, signature_entry,
};
use aruna_core::structs::execution::job::{
    ImportReportDetail, ImportReportRow, ReasonCode, rocrate_plan_key,
};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::blob::{BucketInfo, bucket_permission_path};
use aruna_core::structs::storage::distribution::hint::{SetHint, with_hint};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use super::reports::{system_write, written};
use super::rewrite::validate_document;
use super::{
    ImportCheckpoint, ImportEntryPlan, ImportFailure, ImportPhase, ImportPlan, ImportRun,
    bucket_call, bucket_result, ensure_valid_path, job_call, permission_call, permission_result,
    precheck_size, preflight_crate, target_key, unexpected, validation_failure,
};
use crate::federation::import::{ImportError, RecheckImportOperation};
use crate::jobs::run_rows::{SettledRowsOperation, state_row};
use crate::jobs::store::mutation_error::JobMutationError;

/// The call that rechecks an import of another realm: its binding and policies still hold.
pub(super) fn federated_call(run: &ImportRun, now_ms: u64) -> IoCall {
    let now = now_ms / 1000;
    let recheck = RecheckImportOperation::new((*run.spec).clone(), run.owner_node, now);
    IoCall::child(recheck)
}

/// The answer to [`federated_call`].
pub(super) fn federated_result(result: IoResult) -> Result<(), ImportFailure> {
    (result.child::<(), ImportError>()?).map_err(|error| match error {
        ImportError::Storage(error) => ImportFailure::Retryable(error),
        ImportError::Expired => ImportFailure::Deferred(error.to_string()),
        error => ImportFailure::Permanent(error.to_string()),
    })
}

/// WRITE on the document the import creates or updates.
pub(super) fn metadata_permission_call(run: &ImportRun) -> IoCall {
    let spec = &run.spec;
    let path = MetadataRegistryRecord::permission_path_for(
        &spec.auth_context.realm_id,
        spec.metadata.group_id,
        &spec.metadata.path,
        spec.document_id,
    );
    permission_call(&spec.auth_context, path, Permission::WRITE)
}

/// Checks the stored source against its record and reads the archive's layout and metadata.
pub(super) fn inspect_call(
    run: &ImportRun,
    checkpoint: &ImportCheckpoint,
) -> Result<IoCall, ImportFailure> {
    let spec = &run.spec;
    let input = (checkpoint.input.as_ref())
        .ok_or_else(|| ImportFailure::Permanent("import source is missing".to_string()))?;
    precheck_size(input.size, spec.limits.import_source_bytes)?;
    if input.location.get_blake3() != Some(input.blake3.as_slice()) {
        return Err(ImportFailure::Permanent(
            "hidden import source hash does not match its record".to_string(),
        ));
    }
    Ok(job_call(JobEffect::InspectArchive(Box::new(ArchiveRead {
        location: input.location.clone(),
        size: input.size,
        eln: input.eln,
        limits: spec.limits.clone(),
        metadata_bytes: Some(spec.limits.metadata_bytes),
    }))))
}

/// The answer to [`inspect_call`].
pub(super) fn inspected(result: IoResult) -> Result<(ArchiveInspection, String), ImportFailure> {
    match result.event()? {
        Event::Job(JobEvent::ArchiveInspected(found)) => {
            let (inspection, metadata) = found.map_err(ImportFailure::Permanent)?;
            let metadata = metadata
                .ok_or_else(|| ImportFailure::Permanent("RO-Crate metadata is missing".into()))?;
            Ok((inspection, metadata))
        }
        other => Err(unexpected("the inspected archive", &other).into()),
    }
}

/// The version id planned for entry `index`, carrying the destination's set hint.
fn planned_version(run: &ImportRun, now_ms: u64, index: u64, hint: Option<SetHint>) -> Ulid {
    let random = (u128::from(run.seed) << 32) | u128::from(index);
    let fresh = Ulid::from_parts(now_ms, random);
    match hint {
        Some(hint) => with_hint(fresh, hint),
        None => fresh,
    }
}

/// Matches the archive to the crate: each file entity names one payload, and every payload gets
/// a key and a version.
fn plan_entries(
    run: &ImportRun,
    inspection: &ArchiveInspection,
    jsonld: &str,
    (now_ms, hint): (u64, Option<SetHint>),
) -> Result<Vec<ImportEntryPlan>, ImportFailure> {
    let spec = &run.spec;
    let validated = validate_document(jsonld).map_err(validation_failure)?;
    let payload = payload_entries(inspection);
    let mut described = HashMap::<String, String>::new();
    for file_id in &validated.file_ids {
        let Some(candidates) = file_id_candidates(file_id).map_err(ImportFailure::Permanent)?
        else {
            continue;
        };
        let matches = (candidates.iter())
            .filter(|candidate| payload.contains_key(candidate.as_str()))
            .collect::<Vec<_>>();
        let archive_path = match matches.as_slice() {
            [archive_path] => (*archive_path).clone(),
            [] => {
                return Err(ImportFailure::Permanent(format!(
                    "File entity `{file_id}` has no archive payload"
                )));
            }
            _ => {
                return Err(ImportFailure::Permanent(format!(
                    "File entity `{file_id}` maps to multiple archive payloads"
                )));
            }
        };
        if let Some(existing) = described.insert(archive_path.clone(), file_id.clone()) {
            return Err(ImportFailure::Permanent(format!(
                "archive payload `{archive_path}` is described by `{existing}` and `{file_id}`"
            )));
        }
    }
    let mut entries = Vec::with_capacity(payload.len());
    for (index, (path, entry)) in payload.into_iter().enumerate() {
        let target_key = target_key(&spec.target.prefix, &path, spec.limits.key_bytes)?;
        let described_id = described.remove(&path);
        entries.push(ImportEntryPlan {
            archive_path: entry.archive_path.clone(),
            path,
            target_key,
            code: match described_id.is_some() {
                true => ReasonCode::Imported,
                false => ReasonCode::Unlisted,
            },
            described_id,
            version_id: planned_version(run, now_ms, index as u64, hint),
            data_offset: entry.data_offset,
            compressed_size: entry.compressed_size,
            uncompressed_size: entry.uncompressed_size,
            crc32: entry.crc32,
            compression: entry.compression,
        });
    }
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    preflight_crate(spec, run.owner_node, validated.value, &entries)?;
    Ok(entries)
}

/// The checkpoint and the plan a validated source leaves.
#[derive(Debug, PartialEq)]
pub(super) struct Validated {
    pub(super) checkpoint: ImportCheckpoint,
    pub(super) plan: ImportPlan,
}

#[derive(Debug, PartialEq)]
enum Step {
    Bucket,
    BucketAllowed(BucketInfo),
    MetadataAllowed(BucketInfo),
    Federated(BucketInfo),
    Signature,
    Plan,
}

/// Checks the import's targets, plans its entries and stores the plan.
#[derive(Debug, PartialEq)]
pub(super) struct ValidateSourceOperation {
    run: ImportRun,
    checkpoint: ImportCheckpoint,
    plan: Option<ImportPlan>,
    now_ms: u64,
    step: Step,
}

impl ValidateSourceOperation {
    pub(super) fn new(run: ImportRun, checkpoint: ImportCheckpoint) -> Self {
        Self {
            run,
            checkpoint,
            plan: None,
            now_ms: 0,
            step: Step::Bucket,
        }
    }

    /// Plans the entries once every target check passed.
    fn plan(&mut self, bucket: BucketInfo) -> Result<Next<Validated>, ImportFailure> {
        ensure_valid_path(&self.run.spec)?;
        let inspection = (self.checkpoint.inspection.as_ref())
            .ok_or_else(|| ImportFailure::Permanent("archive inspection is missing".into()))?;
        let jsonld = (self.checkpoint.metadata_json.as_ref())
            .ok_or_else(|| ImportFailure::Permanent("RO-Crate metadata is missing".into()))?;
        let hint = bucket.distribution.as_ref().map(|binding| binding.hint());
        let entries = plan_entries(&self.run, inspection, jsonld, (self.now_ms, hint))?;
        self.plan = Some(ImportPlan {
            metadata_json: jsonld.clone(),
            entries,
        });
        match signature_entry(inspection) {
            Some(signature) => {
                let row = ImportReportRow {
                    entry_key: format!("signature/{}", signature.path),
                    code: ReasonCode::SignatureDropped,
                    message: Some("detached signature is not retained after rewriting".to_string()),
                    detail: ImportReportDetail {
                        archive_path: signature.archive_path.clone(),
                        target_key: None,
                        version_id: None,
                        blake3: None,
                        size: None,
                        arn: None,
                        w3id: None,
                        validation: None,
                    },
                };
                self.step = Step::Signature;
                Ok(Next::Call(system_write(&self.run, &row)?))
            }
            None => self.store_plan(),
        }
    }

    fn store_plan(&mut self) -> Result<Next<Validated>, ImportFailure> {
        let plan = self.plan.as_ref().ok_or_else(lost_plan)?;
        let key = rocrate_plan_key(self.run.job_id);
        let row = state_row(key, plan).map_err(ImportFailure::Retryable)?;
        self.step = Step::Plan;
        let (job_id, token) = (self.run.job_id, self.run.claim_token);
        let rows = SettledRowsOperation::new(job_id, token, vec![row], Vec::new());
        Ok(Next::Call(IoCall::child(rows)))
    }
}

fn lost_plan() -> ImportFailure {
    ImportFailure::Retryable("the import plan was lost".to_string())
}

impl Operation for ValidateSourceOperation {
    type Output = Validated;
    type Error = ImportFailure;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Validated>, ImportFailure> {
        Ok(Next::Call(bucket_call(&self.run.spec.target.bucket)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Validated>, ImportFailure> {
        let spec = self.run.spec.clone();
        match std::mem::replace(&mut self.step, Step::Plan) {
            Step::Bucket => {
                let bucket = bucket_result(&spec.target.bucket, result)?;
                let path = bucket_permission_path(
                    spec.auth_context.realm_id,
                    bucket.group_id,
                    self.run.owner_node,
                    &spec.target.bucket,
                );
                self.step = Step::BucketAllowed(bucket);
                let check = permission_call(&spec.auth_context, path, Permission::WRITE);
                Ok(Next::Call(check))
            }
            Step::BucketAllowed(bucket) => {
                permission_result(result)?;
                self.step = Step::MetadataAllowed(bucket);
                Ok(Next::Call(metadata_permission_call(&self.run)))
            }
            Step::MetadataAllowed(bucket) => {
                permission_result(result)?;
                self.step = Step::Federated(bucket);
                Ok(Next::Call(federated_call(&self.run, self.now_ms)))
            }
            Step::Federated(bucket) => {
                federated_result(result)?;
                self.plan(bucket)
            }
            Step::Signature => {
                written(result)?;
                self.store_plan()
            }
            Step::Plan => {
                (result.child::<(), JobMutationError>()?)
                    .map_err(|error| ImportFailure::Retryable(error.to_string()))?;
                let plan = self.plan.take().ok_or_else(lost_plan)?;
                let mut checkpoint = self.checkpoint.clone();
                checkpoint.inspection = None;
                checkpoint.metadata_json = None;
                checkpoint.entries_total = plan.entries.len() as u64;
                checkpoint.phase = ImportPhase::Write;
                Ok(Next::Done(Validated { checkpoint, plan }))
            }
        }
    }
}

#[cfg(test)]
#[path = "plan_tests.rs"]
mod tests;
