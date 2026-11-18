//! Reads and writes the report rows of an import job and marks the entries it never reached.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, VecDeque};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::JOB_ENTRY_KEYSPACE;
use aruna_core::metadata::MetadataValidationViolation;
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::execution::job::{
    ImportReportDetail, ImportReportRow, ReasonCode, SYSTEM_ENTRY_PREFIX, job_entry_key,
};
use byteview::ByteView;

use super::{
    ImportEntryPlan, ImportFailure, ImportPhase, ImportPlan, ImportRun, entry_report, phase_name,
    system_report_key,
};
use crate::jobs::run_rows::SettledEntryOperation;
use crate::jobs::store::JobMutationError;
use crate::jobs::store::row_calls;

/// Report rows one read returns.
const REPORT_PAGE: usize = 512;

/// Reads the report row of `entry_key`.
pub(super) fn report_read(run: &ImportRun, entry_key: &str) -> IoCall {
    let key = job_entry_key(run.job_id, entry_key.as_bytes());
    IoCall::Read(vec![(JOB_ENTRY_KEYSPACE.to_string(), key)])
}

/// The answer to [`report_read`].
pub(super) fn report_of(result: IoResult) -> Result<Option<ImportReportRow>, ImportFailure> {
    let [value] = values::<1>(result)?;
    value
        .map(|value| postcard::from_bytes(value.as_ref()))
        .transpose()
        .map_err(|error| ImportFailure::Retryable(error.to_string()))
}

fn write_entry(
    run: &ImportRun,
    key: &[u8],
    row: &ImportReportRow,
) -> Result<IoCall, ImportFailure> {
    let value =
        postcard::to_allocvec(row).map_err(|error| ImportFailure::Retryable(error.to_string()))?;
    let (job_id, token) = (run.job_id, run.claim_token);
    let entry = SettledEntryOperation::new(job_id, token, key, ByteView::from(value));
    Ok(IoCall::child(entry))
}

/// Writes the report row of an archive entry.
pub(super) fn report_write(
    run: &ImportRun,
    row: &ImportReportRow,
) -> Result<IoCall, ImportFailure> {
    write_entry(run, row.entry_key.as_bytes(), row)
}

/// Writes a report row the entry listing leaves out.
pub(super) fn system_write(
    run: &ImportRun,
    row: &ImportReportRow,
) -> Result<IoCall, ImportFailure> {
    write_entry(run, &system_report_key(&row.entry_key), row)
}

/// The answer to a report write.
pub(super) fn written(result: IoResult) -> Result<(), ImportFailure> {
    (result.child::<(), JobMutationError>()?)
        .map_err(|error| ImportFailure::Retryable(error.to_string()))
}

/// The report rows of the job's archive entries, by entry key.
#[derive(Debug, PartialEq)]
pub(super) struct LoadReportsOperation {
    run: ImportRun,
    rows: HashMap<String, ImportReportRow>,
}

impl LoadReportsOperation {
    pub(super) fn new(run: ImportRun) -> Self {
        Self {
            run,
            rows: HashMap::new(),
        }
    }

    fn page(&self, cursor: Option<Vec<u8>>) -> Next<HashMap<String, ImportReportRow>> {
        let page = row_calls::list_job_entries(self.run.job_id, cursor, REPORT_PAGE);
        Next::Call(IoCall::child(page))
    }
}

impl Operation for LoadReportsOperation {
    type Output = HashMap<String, ImportReportRow>;
    type Error = ImportFailure;

    fn first(&mut self) -> Result<Next<Self::Output>, ImportFailure> {
        Ok(self.page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ImportFailure> {
        let (page, next) = (result.child::<row_calls::EntryPage, JobMutationError>()?)
            .map_err(|error| ImportFailure::Retryable(error.to_string()))?;
        for (entry_key, value) in page {
            if entry_key.first() == Some(&SYSTEM_ENTRY_PREFIX) {
                continue;
            }
            let row: ImportReportRow = postcard::from_bytes(value.as_ref())
                .map_err(|error| ImportFailure::Retryable(error.to_string()))?;
            self.rows.insert(row.entry_key.clone(), row);
        }
        Ok(match next {
            Some(next) => self.page(Some(next)),
            None => Next::Done(std::mem::take(&mut self.rows)),
        })
    }
}

/// How the entries an import did not finish are reported.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Unreached {
    /// The import was cancelled.
    Cancelled,
    /// The entry at the position failed with this message; later ones were not attempted.
    WriteFailed(String),
}

#[derive(Debug, PartialEq)]
enum MarkStep {
    Read,
    Write,
}

/// Marks each entry from `next_entry` on as not attempted, the failed one with its message.
#[derive(Debug, PartialEq)]
pub(super) struct MarkEntriesOperation {
    run: ImportRun,
    entries: VecDeque<ImportEntryPlan>,
    failed: Option<String>,
    first: bool,
    step: MarkStep,
}

impl MarkEntriesOperation {
    pub(super) fn new(
        run: ImportRun,
        plan: Option<&ImportPlan>,
        next_entry: usize,
        unreached: Unreached,
    ) -> Self {
        let entries = plan.map_or_else(VecDeque::new, |plan| {
            plan.entries.iter().skip(next_entry).cloned().collect()
        });
        let failed = match unreached {
            Unreached::Cancelled => None,
            Unreached::WriteFailed(message) => Some(message),
        };
        Self {
            run,
            entries,
            failed,
            first: true,
            step: MarkStep::Read,
        }
    }

    fn read_next(&mut self) -> Next<()> {
        self.step = MarkStep::Read;
        match self.entries.front() {
            Some(entry) => Next::Call(report_read(&self.run, &entry.path)),
            None => Next::Done(()),
        }
    }
}

impl Operation for MarkEntriesOperation {
    type Output = ();
    type Error = ImportFailure;

    fn first(&mut self) -> Result<Next<()>, ImportFailure> {
        Ok(self.read_next())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ImportFailure> {
        match self.step {
            MarkStep::Read => {
                let entry = self.entries.pop_front().ok_or_else(|| {
                    ImportFailure::Retryable("no import entry was read".to_string())
                })?;
                let mut row = report_of(result)?.unwrap_or_else(|| entry_report(&entry));
                let current = std::mem::replace(&mut self.first, false);
                let (code, message) = match (&self.failed, current) {
                    (None, _) => (
                        ReasonCode::NotAttempted,
                        "not attempted because the import was cancelled".to_string(),
                    ),
                    (Some(message), true) => (ReasonCode::Failed, message.clone()),
                    (Some(_), false) => (
                        ReasonCode::NotAttempted,
                        "not attempted after an earlier import failure".to_string(),
                    ),
                };
                row.code = code;
                row.message = Some(message);
                self.step = MarkStep::Write;
                Ok(Next::Call(report_write(&self.run, &row)?))
            }
            MarkStep::Write => {
                written(result)?;
                Ok(self.read_next())
            }
        }
    }
}

/// Report rows of the crate metadata file.
fn metadata_detail(validation: Option<MetadataValidationViolation>) -> ImportReportDetail {
    ImportReportDetail {
        archive_path: "ro-crate-metadata.json".to_string(),
        target_key: None,
        version_id: None,
        blake3: None,
        size: None,
        arn: None,
        w3id: None,
        validation,
    }
}

/// The report row of a failed phase.
pub(super) fn phase_row(phase: ImportPhase, message: &str) -> ImportReportRow {
    ImportReportRow {
        entry_key: format!("failure/{}", phase_name(phase)),
        // The phase-failure path only sees a plain string or the panic marker;
        // validation codes travel typed through the validation rows.
        code: ReasonCode::Failed,
        message: Some(message.to_string()),
        detail: metadata_detail(None),
    }
}

/// The report rows of a crate's validation findings.
pub(super) fn validation_rows(violations: &[MetadataValidationViolation]) -> Vec<ImportReportRow> {
    (violations.iter().enumerate())
        .map(|(index, violation)| ImportReportRow {
            entry_key: format!("validation/{index:08}"),
            code: match violation.code == "unsupported_crate_version" {
                true => ReasonCode::UnsupportedCrateVersion,
                false => ReasonCode::Failed,
            },
            message: Some(violation.message.clone()),
            detail: metadata_detail(Some(violation.clone())),
        })
        .collect()
}

/// Writes report rows the entry listing leaves out, one after the other.
#[derive(Debug, PartialEq)]
pub(super) struct SystemRowsOperation {
    run: ImportRun,
    rows: VecDeque<ImportReportRow>,
}

impl SystemRowsOperation {
    pub(super) fn new(run: ImportRun, rows: Vec<ImportReportRow>) -> Self {
        Self {
            run,
            rows: rows.into(),
        }
    }

    fn write_next(&mut self) -> Result<Next<()>, ImportFailure> {
        match self.rows.pop_front() {
            Some(row) => Ok(Next::Call(system_write(&self.run, &row)?)),
            None => Ok(Next::Done(())),
        }
    }
}

impl Operation for SystemRowsOperation {
    type Output = ();
    type Error = ImportFailure;

    fn first(&mut self) -> Result<Next<()>, ImportFailure> {
        self.write_next()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ImportFailure> {
        written(result)?;
        self.write_next()
    }
}

#[cfg(test)]
#[path = "reports_tests.rs"]
mod tests;
