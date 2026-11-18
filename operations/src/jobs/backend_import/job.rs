//! Runs one backend import: lists the rule's folder page by page and registers each object.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::jobs::{JobEffect, JobEvent, JobReport};
use aruna_core::keyspaces::JOB_STATE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobResultPayload;
use aruna_core::structs::storage::backend_import::{BackendImportCheckpoint, ExternalObject};
use aruna_core::structs::storage::blob::BackendRef;
use byteview::ByteView;

use super::object::ImportObjectOperation;
use super::prepare::{PrepareRunOperation, RunEnd};
use super::reads::{PageReadsOperation, TakenPathsOperation};
use super::{
    ImportJob, ImportRun, ItemError, LIST_PAGE, Outcome, PageReads, checkpoint_key, retryable,
    tally,
};
use crate::jobs::blob_request::BlobRequest;
use crate::jobs::executor::JobRunOutcome;
use crate::jobs::page_intents::{IntentReadError, PageIntentsOperation, PathIntents};
use crate::jobs::run_rows::{ReadStateOperation, SettledRowsOperation, StateReadError};
use crate::jobs::store::JobMutationError;

/// Longest time a run of skips goes without a persisted checkpoint.
const SKIP_PERSIST_MS: u64 = 2_000;

#[derive(Debug, PartialEq)]
enum Step {
    Checkpoint,
    Prepare,
    List,
    Intents,
    Reads,
    Taken,
    Stops,
    Object,
    Persist(AfterPersist),
    Reported(AfterPersist),
}

/// Where the run goes on after a persisted checkpoint.
#[derive(Clone, Copy, Debug, PartialEq)]
enum AfterPersist {
    NextObject,
    NextPage,
    Finished,
}

/// Lists the rule's folder and imports each object, checkpointing after every object that did
/// real work and every few seconds over skips.
#[derive(Debug, PartialEq)]
pub struct BackendImportOperation {
    job: ImportJob,
    pub(super) checkpoint: BackendImportCheckpoint,
    run: Option<ImportRun>,
    shared_run: Option<Arc<ImportRun>>,
    objects: VecDeque<ExternalObject>,
    listed: Vec<ExternalObject>,
    next_page: Option<String>,
    current: Option<String>,
    minted: u64,
    now_ms: u64,
    last_persist_ms: Option<u64>,
    step: Step,
}

type Turn = Result<Next<JobRunOutcome>, ImportRunError>;

/// A failure of the run itself; the job's checkpoint stays for a retry.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ImportRunError(String);

impl From<IoError> for ImportRunError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

impl BackendImportOperation {
    pub fn new(job: ImportJob) -> Self {
        Self {
            job,
            checkpoint: BackendImportCheckpoint::default(),
            run: None,
            shared_run: None,
            objects: VecDeque::new(),
            listed: Vec::new(),
            next_page: None,
            current: None,
            minted: 0,
            now_ms: 0,
            last_persist_ms: None,
            step: Step::Checkpoint,
        }
    }

    fn prepare(&mut self) -> Next<JobRunOutcome> {
        self.step = Step::Prepare;
        Next::Call(IoCall::child(PrepareRunOperation::new(self.job.clone())))
    }

    fn run_mut(&mut self) -> Result<&mut ImportRun, ImportRunError> {
        self.run
            .as_mut()
            .ok_or_else(|| ImportRunError("the run lost its page".to_string()))
    }

    fn next_object(&mut self) -> Next<JobRunOutcome> {
        if self.objects.is_empty() {
            return self.page_done();
        }
        self.step = Step::Stops;
        Next::Call(job_call(JobEffect::Stops))
    }

    fn import_next(&mut self) -> Turn {
        let Some(object) = self.objects.pop_front() else {
            return Ok(self.page_done());
        };
        let run = (self.shared_run.clone())
            .ok_or_else(|| ImportRunError("the run lost its page".to_string()))?;
        self.current = Some(object.key.clone());
        self.minted += 1;
        self.step = Step::Object;
        let import = ImportObjectOperation::new(self.job.clone(), run, object, self.minted);
        Ok(Next::Call(IoCall::child(import)))
    }

    fn page_done(&mut self) -> Next<JobRunOutcome> {
        let next = self.next_page.take();
        let after = match next.is_some() {
            true => AfterPersist::NextPage,
            false => AfterPersist::Finished,
        };
        if next.is_some() {
            self.checkpoint.cursor = next;
        }
        self.persist(after)
    }

    /// Writes the checkpoint; a value that does not encode ends the run for a retry.
    fn persist(&mut self, after: AfterPersist) -> Next<JobRunOutcome> {
        let value = match postcard::to_allocvec(&self.checkpoint) {
            Ok(value) => ByteView::from(value),
            Err(error) => return Next::Done(retryable(error)),
        };
        let row = (
            JOB_STATE_KEYSPACE.to_string(),
            checkpoint_key(self.job.job_id),
            value,
        );
        self.step = Step::Persist(after);
        let (job_id, token) = (self.job.job_id, self.job.claim_token);
        let rows = SettledRowsOperation::new(job_id, token, vec![row], Vec::new());
        Next::Call(IoCall::child(rows))
    }

    fn resume(&mut self, after: AfterPersist) -> Next<JobRunOutcome> {
        match after {
            AfterPersist::NextObject => self.next_object(),
            AfterPersist::NextPage => self.prepare(),
            AfterPersist::Finished => {
                let checkpoint = &self.checkpoint;
                Next::Done(JobRunOutcome::Succeeded(JobResultPayload::BackendImport {
                    imported: checkpoint.imported,
                    skipped: checkpoint.skipped,
                    failed: checkpoint.failed,
                }))
            }
        }
    }

    fn counted(&self) -> u64 {
        let checkpoint = &self.checkpoint;
        checkpoint.imported + checkpoint.skipped + checkpoint.failed
    }
}

fn job_call(effect: JobEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Job(effect)))
}

impl Operation for BackendImportOperation {
    type Output = JobRunOutcome;
    type Error = ImportRunError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn {
        let read = ReadStateOperation::<BackendImportCheckpoint>::new(self.job.job_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.step, Step::Stops) {
            Step::Checkpoint => {
                let read = result.child::<Option<BackendImportCheckpoint>, StateReadError>()?;
                match read {
                    Ok(checkpoint) => {
                        self.checkpoint = checkpoint.unwrap_or_default();
                        self.last_persist_ms = Some(self.now_ms);
                        Ok(self.prepare())
                    }
                    Err(error) => Ok(Next::Done(retryable(error))),
                }
            }
            Step::Prepare => match result.child::<ImportRun, RunEnd>()? {
                Ok(run) => {
                    let list = BlobEffect::ListExternal {
                        backend: BackendRef::Group(run.rule.backend_id),
                        prefix: run.rule.backend_prefix.clone(),
                        start_after: self.checkpoint.cursor.clone(),
                        limit: LIST_PAGE,
                    };
                    self.run = Some(run);
                    self.step = Step::List;
                    Ok(Next::Call(IoCall::child(BlobRequest::new(list))))
                }
                Err(RunEnd(outcome)) => Ok(Next::Done(outcome)),
            },
            Step::List => {
                let (objects, next) = match result.child::<BlobEvent, BlobError>()? {
                    Ok(BlobEvent::ExternalListed { objects, next }) => (objects, next),
                    Ok(other) => {
                        let label = Event::Blob(other).label();
                        let error = BlobError::ListError(format!("unexpected answer {label}"));
                        return Ok(Next::Done(retryable(error)));
                    }
                    Err(error) => return Ok(Next::Done(retryable(error))),
                };
                self.next_page = next;
                let import_id = self.run_mut()?.rule.import_id;
                let intents = PageIntentsOperation::new(import_id, &objects);
                self.listed = objects;
                self.step = Step::Intents;
                Ok(Next::Call(IoCall::child(intents)))
            }
            Step::Intents => match result.child::<PathIntents, IntentReadError>()? {
                Ok(intents) => {
                    let run = self.run_mut()?;
                    run.intents = intents;
                    let rule = run.rule.clone();
                    let reads = PageReadsOperation::new(rule, self.listed.clone());
                    self.step = Step::Reads;
                    Ok(Next::Call(IoCall::child(reads)))
                }
                Err(error) => Ok(Next::Done(retryable(error))),
            },
            Step::Reads => match result.child::<PageReads, ItemError>()? {
                Ok(page) => {
                    let run = (self.run.as_mut())
                        .ok_or_else(|| ImportRunError("the run lost its page".to_string()))?;
                    run.page = page;
                    let taken = TakenPathsOperation::new(&run.shared, &self.listed);
                    self.step = Step::Taken;
                    Ok(Next::Call(IoCall::child(taken)))
                }
                Err(error) => Ok(Next::Done(retryable(error.reason()))),
            },
            Step::Taken => match result.child::<HashSet<String>, ItemError>()? {
                Ok(taken) => {
                    let mut run = (self.run.take())
                        .ok_or_else(|| ImportRunError("the run lost its page".to_string()))?;
                    run.page.taken = taken;
                    self.shared_run = Some(Arc::new(run));
                    self.objects = std::mem::take(&mut self.listed).into();
                    Ok(self.next_object())
                }
                Err(error) => Ok(Next::Done(retryable(error.reason()))),
            },
            Step::Stops => {
                let event = result.event()?;
                let Event::Job(JobEvent::Stops(stops)) = event else {
                    return Err(ImportRunError(format!(
                        "unexpected answer {}",
                        event.label()
                    )));
                };
                if stops.cancelled {
                    return Ok(Next::Done(JobRunOutcome::Cancelled));
                }
                if stops.shutdown {
                    return Ok(Next::Done(JobRunOutcome::Interrupted));
                }
                self.import_next()
            }
            Step::Object => {
                let outcome = result.child::<Outcome, ItemError>()?;
                let key = self.current.take().unwrap_or_default();
                let counted = match tally(&mut self.checkpoint, &key, outcome) {
                    Ok(counted) => counted,
                    Err(error) => return Ok(Next::Done(retryable(error))),
                };
                // Real work is persisted at once; skips only every few seconds, to show progress.
                let last = self.last_persist_ms.unwrap_or(self.now_ms);
                if counted || self.now_ms.saturating_sub(last) >= SKIP_PERSIST_MS {
                    return Ok(self.persist(AfterPersist::NextObject));
                }
                Ok(self.next_object())
            }
            Step::Persist(after) => {
                if let Err(error) = result.child::<(), JobMutationError>()? {
                    return Ok(Next::Done(retryable(error)));
                }
                self.last_persist_ms = Some(self.now_ms);
                self.step = Step::Reported(after);
                let report = JobReport {
                    current: Some(self.counted()),
                    ..JobReport::default()
                };
                Ok(Next::Call(job_call(JobEffect::Report(report))))
            }
            Step::Reported(after) => {
                let event = result.event()?;
                if !matches!(event, Event::Job(JobEvent::Reported)) {
                    return Err(ImportRunError(format!(
                        "unexpected answer {}",
                        event.label()
                    )));
                }
                Ok(self.resume(after))
            }
        }
    }
}
