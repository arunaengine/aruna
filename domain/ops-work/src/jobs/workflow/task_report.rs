//! Keeps the report a finished plugin task wrote in its task row on this node, read out of the
//! terminal attempt before any export can see the container's files.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::effect::ComputeAnswer;
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::install_work::{
    Found, MAX_REPORT, TaskRow, WorkLookup, WorkRow, report_text, task_place,
};
use aruna_core::keyspaces::PLUGIN_WORK_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{decode, encode, values};
use aruna_core::types::Key;
use ulid::Ulid;

#[derive(Debug, PartialEq)]
enum ReportPhase {
    Fetch,
    Collect,
    Keep,
}

/// Keeps the report of task `(install_id, work_id)` that `fetch` opens, at most
/// [`MAX_REPORT`] bytes; a missing, too large or unreadable report is kept as none.
#[derive(Debug, PartialEq)]
pub struct KeepReport {
    ids: (Ulid, Ulid),
    fetch: Option<IoCall>,
    phase: ReportPhase,
}

type ReportNext = Result<Next<()>, IoError>;

impl KeepReport {
    pub fn new(ids: (Ulid, Ulid), fetch: IoCall) -> Self {
        Self {
            ids,
            fetch: Some(fetch),
            phase: ReportPhase::Fetch,
        }
    }

    fn keep(&mut self, bytes: &[u8]) -> ReportNext {
        let report = report_text(bytes).filter(|text| !text.is_empty());
        self.phase = ReportPhase::Keep;
        Ok(Next::Call(IoCall::child(StoreReport::new(
            self.ids, report,
        ))))
    }
}

impl Operation for KeepReport {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> ReportNext {
        Ok(Next::Call(self.fetch.take().ok_or(IoError::Unfinished)?))
    }

    fn next(&mut self, result: IoResult) -> ReportNext {
        match self.phase {
            ReportPhase::Fetch => match result.event()? {
                Event::Compute(answer) => match *answer {
                    ComputeAnswer::Output(Ok(body)) if body.size <= MAX_REPORT as u64 => {
                        self.phase = ReportPhase::Collect;
                        let collect = BlobEffect::CollectStream {
                            blob: body.bytes,
                            limit: Some(MAX_REPORT as u64),
                        };
                        Ok(Next::Call(Effect::blob(collect).into()))
                    }
                    _ => self.keep(&[]),
                },
                _ => self.keep(&[]),
            },
            ReportPhase::Collect => match result.blob()? {
                BlobEvent::StreamCollected { bytes: Ok(bytes) } => self.keep(&bytes),
                _ => self.keep(&[]),
            },
            ReportPhase::Keep => {
                if let Err(error) = result.child::<(), IoError>()? {
                    tracing::warn!(error = %error, "A plugin task's report was not kept");
                }
                Ok(Next::Done(()))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum StorePhase {
    Lookup(WorkLookup),
    Task(Vec<u8>, Box<WorkRow>),
    Written,
}

/// Writes `report` into the task row of work `(install_id, work_id)` in a transaction that reads
/// the work row and the task row again; without either row nothing is written.
#[derive(Debug, PartialEq)]
pub struct StoreReport {
    ids: (Ulid, Ulid),
    report: Option<String>,
    phase: StorePhase,
}

impl StoreReport {
    pub fn new((install_id, work_id): (Ulid, Ulid), report: Option<String>) -> Self {
        Self {
            ids: (install_id, work_id),
            report,
            phase: StorePhase::Lookup(WorkLookup::new(install_id, work_id)),
        }
    }
}

impl Operation for StoreReport {
    type Output = ();
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> ReportNext {
        let StorePhase::Lookup(lookup) = &self.phase else {
            return Err(IoError::Unfinished);
        };
        Ok(Next::Call(lookup.ask()))
    }

    fn next(&mut self, result: IoResult) -> ReportNext {
        match std::mem::replace(&mut self.phase, StorePhase::Written) {
            StorePhase::Lookup(mut lookup) => match lookup.take(result)? {
                Found::Ask(ask) => {
                    self.phase = StorePhase::Lookup(lookup);
                    Ok(Next::Call(ask))
                }
                Found::Row(None) => Ok(Next::Done(())),
                Found::Row(Some((key, row))) => {
                    let (space, task_key) = task_place(self.ids.0, self.ids.1);
                    self.phase = StorePhase::Task(key, Box::new(row));
                    Ok(Next::Call(IoCall::Read(vec![(
                        space.to_string(),
                        Key::from(task_key),
                    )])))
                }
            },
            StorePhase::Task(key, row) => {
                let [task] = values::<1>(result)?;
                let Some(task) = task else {
                    return Ok(Next::Done(()));
                };
                let mut task: TaskRow = decode(&task)?;
                task.report = self.report.take();
                let (space, task_key) = task_place(self.ids.0, self.ids.1);
                let rows = vec![
                    (
                        PLUGIN_WORK_KEYSPACE.to_string(),
                        Key::from(key),
                        encode(&*row)?,
                    ),
                    (space.to_string(), Key::from(task_key), encode(&task)?),
                ];
                Ok(Next::Call(IoCall::Write(rows)))
            }
            StorePhase::Written => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
#[path = "task_report_tests.rs"]
mod tests;
