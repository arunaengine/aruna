//! Owns the backend import rules of a group: their checks, writes, removal and reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod change;
pub mod create;
pub mod cron;
pub mod discovery;
pub mod folders;
pub mod remove;
pub mod schedule;
pub mod shared;

use crate::groups::backends::RecordReadError;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, IMPORT_PATTERN_KEYSPACE, JOB_KEYSPACE, JOB_STATE_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, read_row, values};
use aruna_core::structs::execution::job::{JobError, JobId, JobRecord, JobState, job_record_key};
use aruna_core::structs::identity::group_delete::GroupWriteError;
use aruna_core::structs::storage::backend_import::{
    BackendImport, BackendImportCheckpoint, import_key, object_prefix, read_patterns,
};
use aruna_core::structs::storage::blob::ensure_confined_path;
use aruna_core::types::{GroupId, Key, Value};
use std::path::Path;
use thiserror::Error;
use ulid::Ulid;

pub use folders::folders_overlap;

const RULE_PAGE: usize = 256;

#[derive(Debug, Error, PartialEq)]
pub enum ImportRuleError {
    #[error(transparent)]
    GroupWrite(#[from] GroupWriteError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Record(#[from] RecordReadError),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    #[error("import rule not found")]
    NotFound,
    #[error("permission denied")]
    Forbidden,
    #[error("storage read failed: {0}")]
    Read(String),
    #[error(transparent)]
    Submit(#[from] crate::jobs::submit::SubmitJobError),
    #[error("unexpected event in state {0}")]
    Unexpected(&'static str),
    #[error("import rule storage failed: {0}")]
    Io(IoError),
}

impl From<IoError> for ImportRuleError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

/// The run a rule last started, with its counts and kept errors so far.
#[derive(Clone, Debug, PartialEq)]
pub struct LastRun {
    pub job_id: JobId,
    pub state: JobState,
    pub started_ms: u64,
    pub checkpoint: BackendImportCheckpoint,
    /// Why the run as a whole did not finish, such as a bucket it could not list.
    pub error: Option<String>,
}

/// A short fixed reason why a run stopped as a whole, only for a run that ended without
/// success. Provider text can carry signed URLs, so it never leaves the job record.
pub fn run_error(state: JobState, error: Option<&JobError>) -> Option<String> {
    if !matches!(state, JobState::Failed | JobState::Indeterminate) {
        return None;
    }
    let text = error?.message.to_ascii_lowercase();
    let has = |words: &[&str]| words.iter().any(|word| text.contains(word));
    let reason = if has(&["permission", "denied", "forbidden", "unauthorized"]) {
        "No access to the bucket"
    } else if has(&["not found", "nosuchbucket", "does not exist"]) {
        "Bucket not found"
    } else if has(&[
        "timed out",
        "timeout",
        "connect",
        "unreachable",
        "dns",
        "unavailable",
    ]) {
        "Storage could not be reached"
    } else {
        "The run stopped with an error"
    };
    Some(reason.to_string())
}

/// The run a rule last started; `None` before the first run and after its job record was pruned.
#[derive(Debug, PartialEq)]
pub struct LastRunOperation {
    run: Option<(JobId, u64)>,
}

impl LastRunOperation {
    pub fn new(rule: &BackendImport) -> Self {
        Self {
            run: rule.last_job.zip(rule.last_started_ms),
        }
    }
}

impl Operation for LastRunOperation {
    type Output = Option<LastRun>;
    type Error = ImportRuleError;

    fn first(&mut self) -> Result<Next<Option<LastRun>>, ImportRuleError> {
        let Some((job_id, _)) = self.run else {
            return Ok(Next::Done(None));
        };
        Ok(Next::Call(IoCall::Read(vec![
            (JOB_KEYSPACE.to_string(), job_record_key(job_id)),
            (
                JOB_STATE_KEYSPACE.to_string(),
                job_id.to_bytes().to_vec().into(),
            ),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<LastRun>>, ImportRuleError> {
        let (job_id, started_ms) = self.run.ok_or(ImportRuleError::Unexpected("LastRun"))?;
        let [record, checkpoint] = values::<2>(result)?;
        let Some(record) = record else {
            return Ok(Next::Done(None));
        };
        // A malformed job row is retained for offline recovery and reads as absent.
        let record = match JobRecord::from_bytes(&record) {
            Ok(record) => record,
            Err(error) => {
                tracing::warn!(job_id = %job_id, error = %error, "Malformed job record retained");
                return Ok(Next::Done(None));
            }
        };
        let checkpoint = (checkpoint.as_deref())
            .map(postcard::from_bytes::<BackendImportCheckpoint>)
            .transpose()
            .map_err(|error| ImportRuleError::Read(error.to_string()))?
            .unwrap_or_default();
        Ok(Next::Done(Some(LastRun {
            job_id,
            state: record.state,
            started_ms,
            checkpoint,
            error: run_error(record.state, record.last_error.as_ref()),
        })))
    }
}

/// A folder is empty or a relative path that ends with `/`, so a prefix never matches
/// part of a name.
pub fn validate_folder(field: &str, folder: &str) -> Result<(), ImportRuleError> {
    let invalid = |reason: &str| ImportRuleError::Invalid(format!("{field} {reason}"));
    if folder.is_empty() {
        return Ok(());
    }
    if folder.trim() != folder {
        return Err(invalid("must not start or end with spaces"));
    }
    let dot = folder.split('/').any(|segment| segment == ".");
    if !folder.ends_with('/') || folder.starts_with('/') || folder.contains("//") || dot {
        return Err(invalid(
            "must be empty or a relative folder that ends with `/`",
        ));
    }
    ensure_confined_path(Path::new(folder)).map_err(|error| invalid(&error.to_string()))
}

/// Why a bucket linked whole to a provider bucket keeps raw bytes.
pub const MAPPED_REASON: &str =
    "This bucket is stored 1:1 in a provider bucket, so it cannot be compressed or encrypted.";

/// Pages through a group's rules, in the caller's transaction, for a whole-bucket provider
/// link of one bucket.
#[derive(Debug, PartialEq)]
pub(crate) struct LinkScan {
    group_id: GroupId,
    bucket: String,
    pub(crate) linked: bool,
}

impl LinkScan {
    pub(crate) fn new(group_id: GroupId, bucket: &str) -> Self {
        Self {
            group_id,
            bucket: bucket.to_string(),
            linked: false,
        }
    }

    pub(crate) fn page(&self, after: Option<Key>) -> IoCall {
        IoCall::Page {
            space: BACKEND_IMPORT_KEYSPACE.to_string(),
            prefix: self.group_id.to_bytes().to_vec().into(),
            after,
            limit: RULE_PAGE,
        }
    }

    /// The next page while one is left and no link is found; `None` ends the scan.
    pub(crate) fn scanned(
        &mut self,
        values: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Result<Option<IoCall>, ConversionError> {
        for (_, value) in values {
            let rule = BackendImport::from_bytes(&value)?;
            self.linked |= rule.bucket == self.bucket && rule.mapped_bucket().is_some();
        }
        Ok(next
            .filter(|_| !self.linked)
            .map(|next| self.page(Some(next))))
    }
}

/// The rules of one group, or of every group on this node for the schedule.
#[derive(Debug, PartialEq)]
pub struct ListImportsOperation {
    pages: PrefixPages,
    rules: Vec<BackendImport>,
}

impl ListImportsOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self::under(group_id.to_bytes().to_vec())
    }

    pub fn all() -> Self {
        Self::under(Vec::new())
    }

    fn under(prefix: Vec<u8>) -> Self {
        Self {
            pages: PrefixPages::new(BACKEND_IMPORT_KEYSPACE, prefix, RULE_PAGE),
            rules: Vec::new(),
        }
    }
}

impl Operation for ListImportsOperation {
    type Output = Vec<BackendImport>;
    type Error = ImportRuleError;

    fn first(&mut self) -> Result<Next<Vec<BackendImport>>, ImportRuleError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<BackendImport>>, ImportRuleError> {
        for (_, value) in self.pages.take(result)? {
            self.rules.push(BackendImport::from_bytes(value.as_ref())?);
        }
        if self.pages.done() {
            return Ok(Next::Done(std::mem::take(&mut self.rules)));
        }
        Ok(Next::Call(self.pages.call()))
    }
}

/// One rule of a group, if it exists.
#[derive(Debug, PartialEq)]
pub struct ReadImportOperation {
    key: Key,
}

impl ReadImportOperation {
    pub fn new(group_id: GroupId, import_id: Ulid) -> Self {
        Self {
            key: import_key(group_id, import_id),
        }
    }
}

impl Operation for ReadImportOperation {
    type Output = Option<BackendImport>;
    type Error = ImportRuleError;

    fn first(&mut self) -> Result<Next<Option<BackendImport>>, ImportRuleError> {
        let read = read_row(BACKEND_IMPORT_KEYSPACE, self.key.clone());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<BackendImport>>, ImportRuleError> {
        let rule = result.value()?;
        let rule = rule.as_deref().map(BackendImport::from_bytes).transpose()?;
        Ok(Next::Done(rule))
    }
}

/// The earlier write patterns of a rule, which still name some of its files.
#[derive(Debug, PartialEq)]
pub struct ReadEarlierOperation {
    import_id: Ulid,
}

impl ReadEarlierOperation {
    pub fn new(import_id: Ulid) -> Self {
        Self { import_id }
    }
}

impl Operation for ReadEarlierOperation {
    type Output = Vec<String>;
    type Error = ImportRuleError;

    fn first(&mut self) -> Result<Next<Vec<String>>, ImportRuleError> {
        let read = read_row(IMPORT_PATTERN_KEYSPACE, object_prefix(self.import_id));
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<String>>, ImportRuleError> {
        let stored = result.value()?;
        Ok(Next::Done(read_patterns(stored.as_deref())?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn import_rule() -> BackendImport {
        crate::s3::bucket::rename::tests::rule()
    }

    fn rule_page(rules: &[BackendImport], next: Option<Key>) -> IoResult {
        IoResult::Page {
            rows: (rules.iter())
                .map(|rule| (rule.key(), rule.to_bytes().unwrap().into()))
                .collect(),
            next,
        }
    }

    #[test]
    fn lists_every_page() {
        let mut operation = ListImportsOperation::all();
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Page { after: None, .. }))
        ));
        let cursor = Key::from(vec![1]);
        let rule = import_rule();
        let turn = operation.next(rule_page(std::slice::from_ref(&rule), Some(cursor.clone())));
        assert!(matches!(
            turn,
            Ok(Next::Call(IoCall::Page { after: Some(after), .. })) if after == cursor
        ));
        let turn = operation.next(rule_page(std::slice::from_ref(&rule), None));
        assert_eq!(turn, Ok(Next::Done(vec![rule.clone(), rule])));
    }

    #[test]
    fn unstarted_rule_no_run() {
        let mut operation = LastRunOperation::new(&import_rule());
        assert_eq!(operation.first(), Ok(Next::Done(None)));
    }

    #[test]
    fn pruned_run_absent() {
        let mut rule = import_rule();
        rule.last_job = Some(JobId::from_bytes([1; 16]));
        rule.last_started_ms = Some(5);
        let mut operation = LastRunOperation::new(&rule);
        assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
        let turn = operation.next(IoResult::Values(vec![None, None]));
        assert_eq!(turn, Ok(Next::Done(None)));
    }

    #[test]
    fn missing_patterns_empty() {
        let mut operation = ReadEarlierOperation::new(Ulid::nil());
        operation.first().unwrap();
        let turn = operation.next(IoResult::Values(vec![None]));
        assert_eq!(turn, Ok(Next::Done(Vec::new())));
    }

    #[test]
    fn hides_provider_text() {
        let signed = "List error: PermissionDenied at list => GET \
                      https://acct.blob.core.windows.net/c?sv=2024&sig=SECRET (persistent)";
        let shown = run_error(JobState::Failed, Some(&JobError::retryable(signed))).unwrap();
        assert_eq!(shown, "No access to the bucket");
        assert!(!shown.contains("SECRET") && !shown.contains("https://"));
        let lost = JobError::retryable("Read error: timed out reading the listing");
        assert_eq!(
            run_error(JobState::Indeterminate, Some(&lost)).as_deref(),
            Some("Storage could not be reached")
        );
        let odd = JobError::permanent("sig=SECRET");
        assert_eq!(
            run_error(JobState::Failed, Some(&odd)).as_deref(),
            Some("The run stopped with an error")
        );
        // A retry that later succeeded leaves its old error behind.
        assert_eq!(run_error(JobState::Succeeded, Some(&odd)), None);
        assert_eq!(run_error(JobState::Running, Some(&odd)), None);
        assert_eq!(run_error(JobState::Failed, None), None);
    }

    #[test]
    fn checks_folders() {
        assert!(validate_folder("folder", "").is_ok());
        assert!(validate_folder("folder", "raw/2026/").is_ok());
        for folder in [
            "raw",
            "/raw/",
            "raw//x/",
            "../raw/",
            "raw/../x/",
            " raw/",
            "raw/ ",
            "raw/./",
            "./raw/",
        ] {
            assert!(validate_folder("folder", folder).is_err(), "{folder}");
        }
        assert!(folders_overlap("", "raw/"));
        assert!(folders_overlap("raw/x/", "raw/"));
        assert!(!folders_overlap("raw/", "rawer/"));
    }
}
