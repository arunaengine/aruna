//! Writes the export ZIP into a hidden blob while its entries stream in from their sources.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, EffectGroup, GroupForm};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, GroupEvent, GroupResult};
use aruna_core::jobs::{JobEffect, JobEvent, ZipFailure, ZipWork};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::execution::job::ArtifactRef;

use super::archive::snapshot_moment;
use super::open::OpenCandidateOperation;
use super::sources::stop_failure;
use super::{
    CandidateOpen, ExportCheckpoint, ExportFailure, ExportScope, METADATA_PATH, OpenStatus,
    PlannedEntry, ProbedEntry, REPORT_PATH, SourceScope,
};
use crate::jobs::import::{hidden_delete, job_call, stops_of, unexpected};
use crate::replication::bao_read::BaoReadOutput;

/// The archive parts an export writes, in entry order after sorting by path.
#[derive(Debug, PartialEq)]
struct ArchiveParts {
    metadata: Vec<u8>,
    entries: Vec<PlannedEntry>,
    report: Option<Vec<u8>>,
    snapshot_ms: u64,
}

fn archive_parts(
    checkpoint: &ExportCheckpoint,
    opened: Vec<ProbedEntry>,
) -> Result<ArchiveParts, ExportFailure> {
    let metadata = (checkpoint.rewritten_jsonld.clone())
        .ok_or_else(|| ExportFailure::Permanent("rewritten metadata is missing".to_string()))?;
    let snapshot_ms = snapshot_moment(checkpoint)?;
    let mut entries = Vec::with_capacity(opened.len());
    for entry in opened {
        let entity = &checkpoint.entities[entry.entity_index];
        let path = (entity.zip_path.clone())
            .ok_or_else(|| ExportFailure::Permanent("planned ZIP path is missing".to_string()))?;
        let mut candidate =
            (entity.candidates.get(entry.candidate_index).cloned()).ok_or_else(|| {
                ExportFailure::Permanent("planned export candidate is missing".to_string())
            })?;
        candidate.expected_blake3 = Some(entry.hash);
        entries.push(PlannedEntry {
            entity_index: entry.entity_index,
            candidate_index: entry.candidate_index,
            path,
            candidate,
            expected_blake3: entry.hash,
            modified_ms: (entry.resolved_version)
                .map_or(snapshot_ms, |version| version.timestamp_ms()),
        });
    }
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(ArchiveParts {
        metadata,
        entries,
        report: checkpoint.report_json.clone(),
        snapshot_ms,
    })
}

#[derive(Debug, PartialEq)]
enum WriteStep {
    Metadata,
    Open,
    Stream,
    Report,
    Close,
    Stopped,
}

/// Writes the crate metadata, every entry from its source and the report into the archive. A
/// failure drops the writer, which ends the stream the spool reads.
#[derive(Debug, PartialEq)]
pub(super) struct WriteArchiveOperation {
    scope: SourceScope,
    zip: Option<ZipWork>,
    metadata: Vec<u8>,
    entries: VecDeque<PlannedEntry>,
    report: Option<Vec<u8>>,
    snapshot_ms: u64,
    current: Option<PlannedEntry>,
    step: WriteStep,
}

type WriteTurn = Result<Next<()>, ExportFailure>;

fn lost_writer() -> ExportFailure {
    ExportFailure::Retryable("the archive writer is gone".to_string())
}

fn stream_status(corrupt: bool) -> OpenStatus {
    match corrupt {
        true => OpenStatus::Corrupt,
        false => OpenStatus::Offline,
    }
}

impl WriteArchiveOperation {
    fn new(scope: SourceScope, zip: ZipWork, parts: ArchiveParts) -> Self {
        Self {
            scope,
            zip: Some(zip),
            metadata: parts.metadata,
            entries: parts.entries.into(),
            report: parts.report,
            snapshot_ms: parts.snapshot_ms,
            current: None,
            step: WriteStep::Metadata,
        }
    }

    fn write_whole(&mut self, path: &str, bytes: Vec<u8>) -> WriteTurn {
        let zip = self.zip.take().ok_or_else(lost_writer)?;
        Ok(Next::Call(job_call(JobEffect::ZipWhole {
            zip,
            path: path.to_string(),
            modified_ms: self.snapshot_ms,
            bytes,
        })))
    }

    fn written(&mut self, result: IoResult) -> Result<(), ExportFailure> {
        let event = result.event()?;
        let Event::Job(JobEvent::ZipWritten { zip, result }) = event else {
            return Err(unexpected("the written archive entry", &event).into());
        };
        self.zip = zip;
        result.map_err(ExportFailure::Retryable)
    }

    fn next_entry(&mut self) -> WriteTurn {
        let Some(entry) = self.entries.pop_front() else {
            return match self.report.take() {
                Some(report) => {
                    self.step = WriteStep::Report;
                    self.write_whole(REPORT_PATH, report)
                }
                None => self.close(),
            };
        };
        let candidate = entry.candidate.clone();
        self.current = Some(entry);
        let open = OpenCandidateOperation::new(self.scope.clone(), candidate, false);
        self.step = WriteStep::Open;
        Ok(Next::Call(IoCall::child(open)))
    }

    fn close(&mut self) -> WriteTurn {
        let zip = self.zip.take().ok_or_else(lost_writer)?;
        self.step = WriteStep::Close;
        Ok(Next::Call(job_call(JobEffect::ZipClose(zip))))
    }

    fn entry(&self) -> Result<&PlannedEntry, ExportFailure> {
        self.current.as_ref().ok_or_else(lost_writer)
    }

    fn candidate_failed(&self, status: OpenStatus, message: String) -> ExportFailure {
        let (entity_index, candidate_index) = (self.current.as_ref())
            .map_or((0, 0), |entry| (entry.entity_index, entry.candidate_index));
        ExportFailure::Candidate {
            entity_index,
            candidate_index,
            status,
            message,
        }
    }

    fn corrupt(&self) -> ExportFailure {
        let path = self
            .current
            .as_ref()
            .map_or("", |entry| entry.path.as_str());
        let message = format!("payload integrity check failed for `{path}`");
        self.candidate_failed(OpenStatus::Corrupt, message)
    }

    fn opened(&mut self, open: CandidateOpen) -> WriteTurn {
        let entry = self.entry()?;
        let (path, expected, modified_ms) =
            (entry.path.clone(), entry.expected_blake3, entry.modified_ms);
        let stream = match open {
            CandidateOpen::Opened(BaoReadOutput::Stream { blob, blake3, .. })
                if blake3 == expected =>
            {
                blob
            }
            CandidateOpen::Opened(BaoReadOutput::Stream { .. })
            | CandidateOpen::Status(OpenStatus::Corrupt) => return Err(self.corrupt()),
            CandidateOpen::Opened(BaoReadOutput::Metadata { .. }) => {
                return Err(ExportFailure::Retryable(
                    "source open unexpectedly returned metadata".to_string(),
                ));
            }
            CandidateOpen::Status(status) => {
                let message = format!("payload source became unavailable for `{path}`");
                return Err(self.candidate_failed(status, message));
            }
        };
        let zip = self.zip.take().ok_or_else(lost_writer)?;
        self.step = WriteStep::Stream;
        Ok(Next::Call(job_call(JobEffect::ZipStream {
            zip,
            path,
            modified_ms,
            stream,
        })))
    }

    fn streamed(&mut self, result: IoResult) -> WriteTurn {
        let event = result.event()?;
        let Event::Job(JobEvent::ZipStreamed { zip, result }) = event else {
            return Err(unexpected("the streamed archive entry", &event).into());
        };
        self.zip = zip;
        match result {
            Ok(hash) if hash == self.entry()?.expected_blake3 => self.next_entry(),
            Ok(_) => Err(self.corrupt()),
            Err(ZipFailure::Stopped) => {
                self.step = WriteStep::Stopped;
                Ok(Next::Call(job_call(JobEffect::Stops)))
            }
            Err(ZipFailure::Source { corrupt, message }) => {
                Err(self.candidate_failed(stream_status(corrupt), message))
            }
            Err(ZipFailure::Sink(message)) => Err(ExportFailure::Retryable(message)),
        }
    }

    fn advance(&mut self, result: IoResult) -> WriteTurn {
        match std::mem::replace(&mut self.step, WriteStep::Stopped) {
            WriteStep::Metadata => {
                self.written(result)?;
                self.next_entry()
            }
            WriteStep::Open => {
                let open = result.child::<CandidateOpen, ExportFailure>()??;
                self.opened(open)
            }
            WriteStep::Stream => self.streamed(result),
            WriteStep::Report => {
                self.written(result)?;
                self.close()
            }
            WriteStep::Close => {
                let event = result.event()?;
                let Event::Job(JobEvent::ZipClosed(closed)) = event else {
                    return Err(unexpected("the closed archive", &event).into());
                };
                closed.map_err(ExportFailure::Retryable)?;
                Ok(Next::Done(()))
            }
            WriteStep::Stopped => {
                Err(stop_failure(stops_of(result)?).unwrap_or(ExportFailure::Interrupted))
            }
        }
    }
}

impl Operation for WriteArchiveOperation {
    type Output = ();
    type Error = ExportFailure;

    fn first(&mut self) -> WriteTurn {
        let metadata = std::mem::take(&mut self.metadata);
        self.write_whole(METADATA_PATH, metadata)
    }

    fn next(&mut self, result: IoResult) -> WriteTurn {
        let turn = self.advance(result);
        if turn.is_err() {
            self.zip = None;
        }
        turn
    }
}

#[derive(Debug, PartialEq)]
enum AssembleStep {
    Open,
    Group,
    Discard(ExportFailure),
}

/// Spools the archive into a hidden blob of the job and returns it as the export's artifact.
#[derive(Debug, PartialEq)]
pub(super) struct AssembleOperation {
    run: ExportScope,
    parts: Option<ArchiveParts>,
    now_ms: u64,
    step: AssembleStep,
}

type AssembleTurn = Result<Next<ArtifactRef>, ExportFailure>;

/// The outcome of the writer in the group's answer.
fn writer_outcome(result: GroupResult) -> Result<Result<(), ExportFailure>, IoError> {
    let wrong = IoError::Unexpected {
        expected: "the finished archive writer",
        got: "another group result",
    };
    match result {
        GroupResult::Done(event) => match *event {
            Event::Child(outcome) => outcome.take().ok_or(wrong),
            _ => Err(wrong),
        },
        GroupResult::Unfinished | GroupResult::Refused => Err(wrong),
    }
}

impl AssembleOperation {
    pub(super) fn new(
        run: ExportScope,
        checkpoint: &ExportCheckpoint,
        opened: Vec<ProbedEntry>,
    ) -> Result<Self, ExportFailure> {
        Ok(Self {
            run,
            parts: Some(archive_parts(checkpoint, opened)?),
            now_ms: 0,
            step: AssembleStep::Open,
        })
    }

    fn start_group(&mut self, zip: ZipWork, stream: aruna_core::jobs::ByteStream) -> AssembleTurn {
        let parts = self
            .parts
            .take()
            .ok_or_else(|| ExportFailure::Retryable("the archive plan is gone".to_string()))?;
        let spec = &self.run.spec;
        let spool = BlobEffect::SpoolHidden {
            namespace: self.run.job_id.as_ulid(),
            name: "rocrate.zip".to_string(),
            created_by: spec.auth_context.user_id,
            max_bytes: Some(spec.limits.export_artifact_bytes),
            deadline: None,
            blob: stream,
        };
        let writer = WriteArchiveOperation::new(self.run.sources(), zip, parts);
        let writer = boxed_suboperation(writer, |result| Event::Child(ChildOutcome::new(result)));
        self.step = AssembleStep::Group;
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members: vec![Effect::Blob(spool), Effect::SubOperation(writer)],
            limit: 2,
            form: GroupForm::All { deadline: None },
        })))
    }

    fn finished(
        &mut self,
        spooled: GroupResult,
        written: Result<(), ExportFailure>,
    ) -> AssembleTurn {
        let GroupResult::Done(event) = spooled else {
            return Err(ExportFailure::Retryable(
                "the artifact spool did not finish".to_string(),
            ));
        };
        match (*event, written) {
            (
                Event::Blob(BlobEvent::HiddenSpooled {
                    location,
                    blake3,
                    size,
                }),
                Ok(()),
            ) => {
                let retention = self.run.spec.limits.artifact_retention_ms;
                Ok(Next::Done(ArtifactRef {
                    location,
                    blake3,
                    size,
                    expires_at_ms: self.now_ms.saturating_add(retention),
                }))
            }
            (Event::Blob(BlobEvent::HiddenSpooled { location, .. }), Err(error)) => {
                match hidden_delete(&location) {
                    Ok(delete) => {
                        self.step = AssembleStep::Discard(error);
                        Ok(Next::Call(delete))
                    }
                    Err(_) => Err(error),
                }
            }
            (Event::Blob(BlobEvent::Error(BlobError::SizeLimitExceeded { limit })), _) => {
                Err(ExportFailure::Permanent(format!(
                    "assembled ZIP exceeds the {limit} byte artifact limit"
                )))
            }
            (Event::Blob(BlobEvent::Error(_)), Err(ExportFailure::Cancelled)) => {
                Err(ExportFailure::Cancelled)
            }
            (Event::Blob(BlobEvent::Error(_)), Err(ExportFailure::Interrupted)) => {
                Err(ExportFailure::Interrupted)
            }
            (Event::Blob(BlobEvent::Error(error)), _) => {
                Err(ExportFailure::Retryable(error.to_string()))
            }
            (event, _) => Err(ExportFailure::Retryable(format!(
                "unexpected hidden artifact event: {}",
                event.label()
            ))),
        }
    }
}

impl Operation for AssembleOperation {
    type Output = ArtifactRef;
    type Error = ExportFailure;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> AssembleTurn {
        Ok(Next::Call(job_call(JobEffect::OpenZip)))
    }

    fn next(&mut self, result: IoResult) -> AssembleTurn {
        match std::mem::replace(&mut self.step, AssembleStep::Group) {
            AssembleStep::Open => {
                let event = result.event()?;
                let Event::Job(JobEvent::ZipOpened { zip, stream }) = event else {
                    return Err(unexpected("the opened archive", &event).into());
                };
                self.start_group(zip, stream)
            }
            AssembleStep::Group => {
                let IoResult::Group(GroupEvent::Finished { results }) = result else {
                    let got = result.label();
                    let expected = "the finished artifact group";
                    return Err(IoError::Unexpected { expected, got }.into());
                };
                let mut results = results.into_iter();
                let (Some(spooled), Some(written)) = (results.next(), results.next()) else {
                    return Err(ExportFailure::Retryable(
                        "an artifact member is missing".to_string(),
                    ));
                };
                self.finished(spooled, writer_outcome(written)?)
            }
            // The delete is best effort; the failure that caused it decides the outcome.
            AssembleStep::Discard(error) => Err(error),
        }
    }
}

#[cfg(test)]
#[path = "assemble_tests.rs"]
pub(super) mod tests;
