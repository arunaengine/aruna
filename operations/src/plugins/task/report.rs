//! Keeps the report a finished plugin task wrote in its task row on this node, read out of the
//! terminal attempt before any export can see the container's files.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::effect::ComputeAnswer;
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::operation::Operation;
use ulid::Ulid;

use super::super::rows::StoreError;
use super::change::{ChangeTask, Changed, TaskChange};
use super::row::{MAX_REPORT, report_text};

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
    now_ms: u64,
    phase: ReportPhase,
}

type ReportNext = Result<Next<()>, IoError>;

impl KeepReport {
    pub fn new(ids: (Ulid, Ulid), fetch: IoCall) -> Self {
        Self {
            ids,
            fetch: Some(fetch),
            now_ms: 0,
            phase: ReportPhase::Fetch,
        }
    }

    fn keep(&mut self, bytes: &[u8]) -> ReportNext {
        let report = report_text(bytes).filter(|text| !text.is_empty());
        let change = ChangeTask::new(self.ids, TaskChange::Report(report), self.now_ms);
        self.phase = ReportPhase::Keep;
        Ok(Next::Call(IoCall::child(change)))
    }
}

impl Operation for KeepReport {
    type Output = ();
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> ReportNext {
        Ok(Next::Call(self.fetch.take().ok_or(IoError::Unfinished)?))
    }

    fn next(&mut self, result: IoResult) -> ReportNext {
        match self.phase {
            ReportPhase::Fetch => match result.event()? {
                Event::Compute(answer) => match *answer {
                    ComputeAnswer::Output(Ok(body)) if body.size <= MAX_REPORT as u64 => {
                        self.phase = ReportPhase::Collect;
                        let collect = BlobEffect::CollectStream { blob: body.bytes };
                        Ok(Next::Call(Effect::Blob(collect).into()))
                    }
                    _ => self.keep(&[]),
                },
                _ => self.keep(&[]),
            },
            ReportPhase::Collect => match result.event()? {
                Event::Blob(BlobEvent::StreamCollected { bytes: Ok(bytes) }) => self.keep(&bytes),
                _ => self.keep(&[]),
            },
            ReportPhase::Keep => {
                if let Err(error) = result.child::<Changed, StoreError>()? {
                    tracing::warn!(error = %error.0, "A plugin task's report was not kept");
                }
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
#[path = "report_tests.rs"]
mod tests;
