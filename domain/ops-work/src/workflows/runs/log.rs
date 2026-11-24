//! The paged run log of a run, kept apart from the job family: appends in sequence order with a
//! head row, and the reads of every page of a run or of its lineage, which later runs replay.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::RUN_LOG_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::workflow_run::{
    LogHead, decode_page, encode_page, head_key, page_key, run_prefix,
};
use aruna_core::types::Key;
use aruna_workflow_contract::preflight::{RunLogEntry, RunLogPage};
use byteview::ByteView;
use ulid::Ulid;

use crate::workflows::CatalogError;

/// Entries one page holds at most.
pub const PAGE_ENTRIES: u32 = 256;
/// Bytes one page holds at most, unless a single entry is larger.
pub const PAGE_BYTES: usize = 1 << 20;
/// Rows one storage page of a log read takes.
const READ_ROWS: usize = 64;

#[derive(Clone, Debug, PartialEq)]
enum AppendStep {
    Head,
    Page(LogHead),
    Write,
}

/// Appends one entry to a run's log. A repeated `Begin`, or a change at or below the newest
/// stored sequence, is stored once: a resumed coordinator may send them again.
#[derive(Clone, Debug, PartialEq)]
pub struct AppendLogOperation {
    run_id: Ulid,
    entry: RunLogEntry,
    step: AppendStep,
}

impl AppendLogOperation {
    pub fn new(run_id: Ulid, entry: RunLogEntry) -> Self {
        Self {
            run_id,
            entry,
            step: AppendStep::Head,
        }
    }

    fn row(&self, key: Key) -> IoCall {
        IoCall::Read(vec![(RUN_LOG_KEYSPACE.to_string(), key)])
    }

    fn seen(&self, head: &LogHead) -> bool {
        match &self.entry {
            RunLogEntry::Begin { .. } => head.begun,
            RunLogEntry::Change { seq, .. } => head.last_seq.is_some_and(|last| last >= *seq),
        }
    }

    /// Writes the entry into `page` and the head that follows it.
    fn write(&mut self, mut head: LogHead, page: Option<(RunLogPage, usize)>) -> AppendNext {
        let size = serde_json::to_vec(&self.entry)
            .map_err(|error| CatalogError::Storage(error.to_string()))?
            .len();
        let fits = |(page, bytes): &(RunLogPage, usize)| {
            (page.entries.len() as u32) < PAGE_ENTRIES && bytes + size <= PAGE_BYTES
        };
        let mut page = match page.filter(fits) {
            Some((page, _)) => page,
            None => {
                head.pages += 1;
                head.entries = 0;
                RunLogPage::default()
            }
        };
        page.entries.push(self.entry.clone());
        head.entries += 1;
        match &self.entry {
            RunLogEntry::Begin { .. } => head.begun = true,
            RunLogEntry::Change { seq, .. } => head.last_seq = Some(*seq),
        }
        let space = RUN_LOG_KEYSPACE.to_string();
        let writes = vec![
            (
                space.clone(),
                page_key(self.run_id, head.pages - 1),
                ByteView::from(encode_page(&page)?),
            ),
            (
                space,
                head_key(self.run_id),
                ByteView::from(head.to_bytes()?),
            ),
        ];
        self.step = AppendStep::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

type AppendNext = Result<Next<()>, CatalogError>;

impl Operation for AppendLogOperation {
    type Output = ();
    type Error = CatalogError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> AppendNext {
        self.step = AppendStep::Head;
        Ok(Next::Call(self.row(head_key(self.run_id))))
    }

    fn next(&mut self, result: IoResult) -> AppendNext {
        match std::mem::replace(&mut self.step, AppendStep::Write) {
            AppendStep::Head => {
                let head = (result.value()?.map(|value| LogHead::from_bytes(&value)))
                    .transpose()?
                    .unwrap_or_default();
                if self.seen(&head) {
                    return Ok(Next::Done(()));
                }
                if head.pages == 0 {
                    return self.write(head, None);
                }
                let last = page_key(self.run_id, head.pages - 1);
                self.step = AppendStep::Page(head);
                Ok(Next::Call(self.row(last)))
            }
            AppendStep::Page(head) => {
                let value = result.value()?.ok_or(IoError::Unfinished)?;
                let page = decode_page(&value)?;
                self.write(head, Some((page, value.len())))
            }
            AppendStep::Write => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// Every page of a run's log, in order.
#[derive(Debug, PartialEq)]
pub struct LoadLogOperation {
    run_id: Ulid,
    pages: Vec<RunLogPage>,
}

impl LoadLogOperation {
    pub fn new(run_id: Ulid) -> Self {
        Self {
            run_id,
            pages: Vec::new(),
        }
    }

    fn page(&self, after: Option<Key>) -> Next<Vec<RunLogPage>> {
        Next::Call(IoCall::Page {
            space: RUN_LOG_KEYSPACE.to_string(),
            prefix: run_prefix(self.run_id),
            after,
            limit: READ_ROWS,
        })
    }
}

impl Operation for LoadLogOperation {
    type Output = Vec<RunLogPage>;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Vec<RunLogPage>>, CatalogError> {
        Ok(self.page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<RunLogPage>>, CatalogError> {
        let (rows, next) = result.page()?;
        let head = head_key(self.run_id);
        for (key, value) in rows {
            if key != head {
                self.pages.push(decode_page(&value)?);
            }
        }
        match next {
            Some(after) => Ok(self.page(Some(after))),
            None => Ok(Next::Done(std::mem::take(&mut self.pages))),
        }
    }
}

/// The answer of a [`LoadLogOperation`] child.
pub(crate) fn pages_answer(result: IoResult) -> Result<Vec<RunLogPage>, CatalogError> {
    result.child::<Vec<RunLogPage>, CatalogError>()?
}

#[derive(Debug, PartialEq)]
enum LineageStep {
    Run,
    Log(Option<Ulid>),
}

/// The log pages of a run and of every earlier run in its lineage, the oldest run first.
/// By default it walks only resumed runs; `including_repeats` also follows repeated runs for
/// explanations like `why`. A run that was deleted ends the chain.
#[derive(Debug, PartialEq)]
pub struct LineageLogOperation {
    next: Ulid,
    include_repeats: bool,
    seen: Vec<Ulid>,
    runs: Vec<Vec<RunLogPage>>,
    step: LineageStep,
}

impl LineageLogOperation {
    pub fn new(run_id: Ulid) -> Self {
        Self {
            next: run_id,
            include_repeats: false,
            seen: Vec::new(),
            runs: Vec::new(),
            step: LineageStep::Run,
        }
    }

    pub fn including_repeats(run_id: Ulid) -> Self {
        Self {
            include_repeats: true,
            ..Self::new(run_id)
        }
    }

    fn done(&mut self) -> Result<Next<Vec<RunLogPage>>, CatalogError> {
        let runs = std::mem::take(&mut self.runs);
        Ok(Next::Done(runs.into_iter().rev().flatten().collect()))
    }
}

impl Operation for LineageLogOperation {
    type Output = Vec<RunLogPage>;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<Vec<RunLogPage>>, CatalogError> {
        self.seen.push(self.next);
        Ok(Next::Call(super::read_run(self.next)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<RunLogPage>>, CatalogError> {
        match std::mem::replace(&mut self.step, LineageStep::Run) {
            LineageStep::Run => {
                let Some(record) = super::run_answer(result)? else {
                    return self.done();
                };
                let earlier = match self.include_repeats {
                    true => record.resumes_run_id.or(record.repeats_run_id),
                    false => record.resumes_run_id,
                };
                self.step = LineageStep::Log(earlier);
                Ok(Next::Call(IoCall::child(LoadLogOperation::new(
                    record.run_id,
                ))))
            }
            LineageStep::Log(resumes) => {
                self.runs.push(pages_answer(result)?);
                match resumes.filter(|run_id| !self.seen.contains(run_id)) {
                    Some(run_id) => {
                        self.next = run_id;
                        self.first()
                    }
                    None => self.done(),
                }
            }
        }
    }
}
