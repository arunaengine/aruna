//! Hands a finished export to its repository and records its report rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::jobs::{ArchiveRead, JobEffect, JobEvent};
use aruna_core::metadata::{MetadataError, ProfileValidationFinding};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::repository::{LinkFailure, RepositoryDestination, RepositoryRecord};
use aruna_core::rocrate_archive::ArchiveInspection;
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::execution::job::{ArtifactRef, ExportReportRow, ExportRoCrateSpec};
use aruna_core::structs::secondary_id::RegisterIdentifiersSpec;
use byteview::ByteView;

use super::archive::blocking_omissions;
use super::sources::stop_failure;
use super::{ExportCheckpoint, ExportFailure, ExportScope};
use crate::harvest::read_connector::ConnectorView;
use crate::jobs::import::{job_call, stops_of, unexpected};
use crate::jobs::repository::check::{
    CheckContentOperation, CrateCheck, checks_content, profile_failure, unmet,
};
use crate::jobs::repository::connector::ConnectorViewOperation;
use crate::jobs::repository::invenio::export::{
    ExportResult, ExportRun, RepositoryExportOperation,
};
use crate::jobs::repository::{Action, TransferError, ensure_supported};
use crate::jobs::run_rows::{
    SettledEntryOperation, SettledRowsOperation, job_state_key, state_row,
};
use crate::jobs::store::JobMutationError;
use crate::jobs::submit::{SubmitJobError, SubmitJobResult};
use crate::jobs::submit_local::SubmitLocalOperation;
use crate::metadata::profile::validation::{
    MetadataProfilePreview, ProfileAssessOperation, profile_verdict,
};

/// Reads the entries of a stored artifact.
#[derive(Debug, PartialEq)]
struct ReadEntriesOperation {
    read: Option<ArchiveRead>,
}

impl Operation for ReadEntriesOperation {
    type Output = ArchiveInspection;
    type Error = TransferError;

    fn first(&mut self) -> Result<Next<ArchiveInspection>, TransferError> {
        let read = (self.read.take())
            .ok_or_else(|| TransferError::Retryable("the artifact was already read".into()))?;
        let inspect = JobEffect::InspectArchive(Box::new(read));
        Ok(Next::Call(job_call(inspect)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ArchiveInspection>, TransferError> {
        match result.event()? {
            Event::Job(JobEvent::ArchiveInspected(found)) => {
                let (inspection, _) = found.map_err(TransferError::Permanent)?;
                Ok(Next::Done(inspection))
            }
            other => Err(unexpected("the inspected artifact", &other).into()),
        }
    }
}

/// The export artifact's entries, with the limits an export archive may reach; a stop of the job
/// ends the inspection.
#[derive(Debug, PartialEq)]
struct InspectArtifactOperation {
    read: Option<ArchiveRead>,
    stopped: bool,
}

impl InspectArtifactOperation {
    fn new(spec: &ExportRoCrateSpec, artifact: &ArtifactRef) -> Self {
        let mut limits = spec.limits.clone();
        limits.import_source_bytes = spec.limits.export_artifact_bytes;
        limits.expanded_import_bytes = spec.limits.export_artifact_bytes;
        limits.max_entries = limits.max_entries.saturating_add(2);
        Self {
            read: Some(ArchiveRead {
                location: artifact.location.clone(),
                size: artifact.size,
                eln: false,
                limits,
                metadata_bytes: None,
            }),
            stopped: false,
        }
    }
}

impl Operation for InspectArtifactOperation {
    type Output = ArchiveInspection;
    type Error = TransferError;

    fn first(&mut self) -> Result<Next<ArchiveInspection>, TransferError> {
        let read = ReadEntriesOperation {
            read: self.read.take(),
        };
        let child = boxed_suboperation(read, |result| Event::Child(ChildOutcome::new(result)));
        Ok(Next::Call(job_call(JobEffect::UntilStop(child))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ArchiveInspection>, TransferError> {
        if self.stopped {
            return Err(match stops_of(result)?.cancelled {
                true => TransferError::Cancelled,
                false => TransferError::Interrupted,
            });
        }
        match result.event()? {
            Event::Job(JobEvent::Finished(finished)) => match *finished {
                Event::Child(outcome) => {
                    let inspected = IoResult::Child(outcome);
                    let found = inspected.child::<ArchiveInspection, TransferError>()?;
                    Ok(Next::Done(found?))
                }
                other => Err(unexpected("the finished inspection", &other).into()),
            },
            Event::Job(JobEvent::Stopped) => {
                self.stopped = true;
                Ok(Next::Call(job_call(JobEffect::Stops)))
            }
            other => Err(unexpected("the finished inspection", &other).into()),
        }
    }
}

#[derive(Debug, PartialEq)]
enum RepositoryStep {
    Connector,
    Assess {
        kind: RepositoryConnectorKind,
        endpoint: String,
    },
    ContentEntries(RepositoryConnectorKind),
    Content(RepositoryConnectorKind),
    ExportEntries(RepositoryConnectorKind),
    Export,
    Identifiers,
    Refused(String),
}

type RepositoryTurn = Result<Next<ExportCheckpoint>, TransferError>;

/// Publishes the finished export to its repository: checks the crate against the repository's
/// requirements, runs the upload and queues the identifiers the record received.
#[derive(Debug, PartialEq)]
pub(super) struct RepositoryStepOperation {
    run: ExportScope,
    destination: RepositoryDestination,
    checkpoint: ExportCheckpoint,
    now_ms: u64,
    step: RepositoryStep,
}

fn export_artifact(checkpoint: &ExportCheckpoint) -> Result<ArtifactRef, TransferError> {
    (checkpoint.artifact.clone())
        .ok_or_else(|| TransferError::Permanent("export artifact missing".into()))
}

/// The plain message of a refused export; a requirement failure names its findings.
fn refusal_message(error: &TransferError) -> String {
    match error {
        TransferError::Refused(LinkFailure::RequirementsUnmet(findings)) => format!(
            "the crate does not meet the repository's requirements: {}",
            (findings.iter())
                .map(|finding| finding.message.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        ),
        error => error.to_string(),
    }
}

impl RepositoryStepOperation {
    pub(super) fn new(
        run: ExportScope,
        destination: RepositoryDestination,
        checkpoint: ExportCheckpoint,
    ) -> Self {
        Self {
            run,
            destination,
            checkpoint,
            now_ms: 0,
            step: RepositoryStep::Connector,
        }
    }

    fn inspect_call(&self, artifact: &ArtifactRef) -> IoCall {
        IoCall::child(InspectArtifactOperation::new(&self.run.spec, artifact))
    }

    fn connector_read(&mut self, view: ConnectorView) -> RepositoryTurn {
        let kind = view.connector.kind;
        ensure_supported(kind, Action::Publish)?;
        if self.destination.published_id.is_some() {
            ensure_supported(kind, Action::Versions)?;
        }
        if self.checkpoint.repository.is_some() {
            return self.inspect_for_export(kind);
        }
        // The snapshot must still meet the requirements before this job writes remotely.
        let endpoint = view.connector.endpoint;
        let jsonld = self.source_crate()?;
        let check = CrateCheck::new(kind, &endpoint, jsonld)?;
        let assess = ProfileAssessOperation::against(
            self.run.local_node,
            check.profile_iri,
            jsonld.to_string(),
        );
        self.step = RepositoryStep::Assess { kind, endpoint };
        Ok(Next::Call(IoCall::child(assess)))
    }

    fn source_crate(&self) -> Result<&str, TransferError> {
        (self.checkpoint.raw_jsonld.as_deref())
            .ok_or_else(|| TransferError::Permanent("source crate metadata missing".into()))
    }

    fn assessed(
        &mut self,
        kind: RepositoryConnectorKind,
        endpoint: &str,
        preview: Result<MetadataProfilePreview, MetadataError>,
    ) -> RepositoryTurn {
        let status = preview.map(profile_verdict).map_err(profile_failure)?;
        let check = CrateCheck::new(kind, endpoint, self.source_crate()?)?;
        let checked = check.finish(status)?;
        if !checked.ready {
            return Err(unmet(checked.findings));
        }
        if !checks_content(kind)? {
            return self.inspect_for_export(kind);
        }
        let artifact = export_artifact(&self.checkpoint)?;
        self.step = RepositoryStep::ContentEntries(kind);
        Ok(Next::Call(self.inspect_call(&artifact)))
    }

    /// The upload takes the artifact's entries; a missing artifact fails inside the upload,
    /// which records the failure on its link.
    fn inspect_for_export(&mut self, kind: RepositoryConnectorKind) -> RepositoryTurn {
        match export_artifact(&self.checkpoint) {
            Ok(artifact) => {
                self.step = RepositoryStep::ExportEntries(kind);
                Ok(Next::Call(self.inspect_call(&artifact)))
            }
            Err(error) => self.upload(kind, Err(error)),
        }
    }

    fn upload(
        &mut self,
        kind: RepositoryConnectorKind,
        inspection: Result<ArchiveInspection, TransferError>,
    ) -> RepositoryTurn {
        let export = RepositoryExportOperation::new(
            kind,
            ExportRun {
                scope: self.run.scope.clone(),
                job: self.run.job.clone(),
                spec: (*self.run.spec).clone(),
                destination: self.destination.clone(),
            },
            self.checkpoint.clone(),
            inspection,
        );
        self.step = RepositoryStep::Export;
        Ok(Next::Call(IoCall::child(export)))
    }

    /// Queued on every run of this phase, so a failed queue is retried; the key names this job,
    /// so a rerun joins the queued one.
    fn register(&mut self) -> RepositoryTurn {
        let identifiers = (self.checkpoint.repository.as_ref())
            .map(|record: &RepositoryRecord| record.identifiers.clone())
            .unwrap_or_default();
        if identifiers.is_empty() {
            return Ok(Next::Done(self.checkpoint.clone()));
        }
        let spec = &self.run.spec;
        let submit = SubmitLocalOperation::identifiers(
            RegisterIdentifiersSpec {
                document_id: spec.document_id,
                identifiers,
                auth_context: spec.auth_context.clone(),
            },
            self.run.owner_node,
            format!("identifiers/{}", self.run.job_id),
            self.run.scope.node,
            self.now_ms,
        );
        self.step = RepositoryStep::Identifiers;
        Ok(Next::Call(IoCall::child(submit)))
    }

    fn advance(&mut self, result: IoResult) -> RepositoryTurn {
        match std::mem::replace(&mut self.step, RepositoryStep::Export) {
            RepositoryStep::Connector => {
                let view = result.child::<ConnectorView, TransferError>()??;
                self.connector_read(view)
            }
            RepositoryStep::Assess { kind, endpoint } => {
                let preview = result.child::<MetadataProfilePreview, MetadataError>()?;
                self.assessed(kind, &endpoint, preview)
            }
            RepositoryStep::ContentEntries(kind) => {
                let inspection = result.child::<ArchiveInspection, TransferError>()??;
                let artifact = export_artifact(&self.checkpoint)?;
                let stop = self.run.job.stop.clone();
                let content =
                    CheckContentOperation::new(kind, &self.checkpoint, artifact, inspection, stop)?;
                self.step = RepositoryStep::Content(kind);
                Ok(Next::Call(IoCall::child(content)))
            }
            RepositoryStep::Content(kind) => {
                let findings = result.child::<Vec<ProfileValidationFinding>, TransferError>()??;
                if !findings.is_empty() {
                    return Err(unmet(findings));
                }
                self.inspect_for_export(kind)
            }
            RepositoryStep::ExportEntries(kind) => {
                let inspection = result.child::<ArchiveInspection, TransferError>()?;
                self.upload(kind, inspection)
            }
            RepositoryStep::Export => {
                let (stored, exported) = result.child::<ExportResult, TransferError>()??;
                self.checkpoint = stored;
                exported?;
                self.register()
            }
            RepositoryStep::Identifiers => {
                (result.child::<SubmitJobResult, SubmitJobError>()?).map_err(|error| {
                    TransferError::Retryable(format!("queueing identifiers failed: {error}"))
                })?;
                Ok(Next::Done(self.checkpoint.clone()))
            }
            RepositoryStep::Refused(message) => Err(TransferError::Permanent(message)),
        }
    }

    /// A refusal is kept in the checkpoint, so the link can record why the push ended.
    fn failed(&mut self, error: TransferError) -> Result<Next<ExportCheckpoint>, ExportFailure> {
        let message = refusal_message(&error);
        match error {
            TransferError::Permanent(message) => Err(ExportFailure::Permanent(message)),
            TransferError::Retryable(message) => Err(ExportFailure::Retryable(message)),
            TransferError::Cancelled => Err(ExportFailure::Cancelled),
            TransferError::Interrupted => Err(ExportFailure::Interrupted),
            TransferError::Refused(failure) => {
                self.checkpoint.link_failure = Some(failure);
                let key = job_state_key(self.run.job_id);
                let row = state_row(key, &self.checkpoint).map_err(ExportFailure::Retryable)?;
                let (job_id, token) = (self.run.job_id, self.run.claim_token);
                let rows = SettledRowsOperation::new(job_id, token, vec![row], Vec::new());
                self.step = RepositoryStep::Refused(message);
                Ok(Next::Call(IoCall::child(rows)))
            }
        }
    }
}

impl Operation for RepositoryStepOperation {
    type Output = ExportCheckpoint;
    type Error = ExportFailure;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<ExportCheckpoint>, ExportFailure> {
        let complete = self.checkpoint.repository_complete;
        if !complete && blocking_omissions(&self.checkpoint.report) > 0 {
            return Err(ExportFailure::Permanent(
                "repository export requires a complete crate with no omitted files".to_string(),
            ));
        }
        let turn = if complete {
            self.register()
        } else {
            let destination = &self.destination;
            let view = ConnectorViewOperation::new(destination.group_id, destination.connector_id);
            Ok(Next::Call(IoCall::child(view)))
        };
        turn.or_else(|error| self.failed(error))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ExportCheckpoint>, ExportFailure> {
        if let RepositoryStep::Refused(message) = &self.step {
            let message = message.clone();
            (result.child::<(), JobMutationError>()?)
                .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
            return Err(ExportFailure::Permanent(message));
        }
        self.advance(result).or_else(|error| self.failed(error))
    }
}

#[derive(Debug, PartialEq)]
enum RowStep {
    Stops,
    Write,
}

/// Writes report rows of the job one after the other; when `stoppable`, a stop of the job ends
/// the writing.
#[derive(Debug, PartialEq)]
pub(super) struct WriteRowsOperation {
    run: ExportScope,
    rows: VecDeque<ExportReportRow>,
    stoppable: bool,
    step: RowStep,
}

type RowTurn = Result<Next<()>, ExportFailure>;

impl WriteRowsOperation {
    pub(super) fn new(run: ExportScope, rows: Vec<ExportReportRow>, stoppable: bool) -> Self {
        Self {
            run,
            rows: rows.into(),
            stoppable,
            step: RowStep::Stops,
        }
    }

    fn next_row(&mut self) -> RowTurn {
        if self.rows.is_empty() {
            return Ok(Next::Done(()));
        }
        if self.stoppable {
            self.step = RowStep::Stops;
            return Ok(Next::Call(job_call(JobEffect::Stops)));
        }
        self.write_row()
    }

    fn write_row(&mut self) -> RowTurn {
        let Some(row) = self.rows.pop_front() else {
            return Ok(Next::Done(()));
        };
        let value = postcard::to_allocvec(&row)
            .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
        let entry = SettledEntryOperation::new(
            self.run.job_id,
            self.run.claim_token,
            row.entry_key.as_bytes(),
            ByteView::from(value),
        );
        self.step = RowStep::Write;
        Ok(Next::Call(IoCall::child(entry)))
    }
}

impl Operation for WriteRowsOperation {
    type Output = ();
    type Error = ExportFailure;

    fn first(&mut self) -> RowTurn {
        self.next_row()
    }

    fn next(&mut self, result: IoResult) -> RowTurn {
        match self.step {
            RowStep::Stops => match stop_failure(stops_of(result)?) {
                Some(stopped) => Err(stopped),
                None => self.write_row(),
            },
            RowStep::Write => {
                (result.child::<(), JobMutationError>()?)
                    .map_err(|error| ExportFailure::Retryable(error.to_string()))?;
                self.next_row()
            }
        }
    }
}

#[cfg(test)]
#[path = "publish_tests.rs"]
mod tests;
