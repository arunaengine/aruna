//! Owns the backend import rules of a group: their checks, writes, removal and reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod change;
pub mod create;
pub mod remove;
pub mod schedule;

use crate::driver::DriverContext;
use crate::groups::backends::{RecordReadError, parse_read};
use crate::jobs::backend_import::read_checkpoint;
use crate::jobs::store::{iter_prefix_page, read_job_record};
use aruna_core::effects::StorageEffect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::BACKEND_IMPORT_KEYSPACE;
use aruna_core::structs::execution::job::{JobId, JobState};
use aruna_core::structs::identity::group_delete::GroupWriteError;
use aruna_core::structs::storage::backend_import::{
    BackendImport, BackendImportCheckpoint, import_key,
};
use aruna_core::structs::storage::blob::ensure_confined_path;
use aruna_core::types::{GroupId, Key};
use std::path::Path;
use thiserror::Error;
use ulid::Ulid;

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
    #[error("storage read failed: {0}")]
    Read(String),
    #[error(transparent)]
    Submit(#[from] crate::jobs::submit::SubmitJobError),
    #[error("unexpected event in state {0}")]
    Unexpected(&'static str),
}

/// The run a rule last started, with its counts and kept errors so far.
#[derive(Clone, Debug, PartialEq)]
pub struct LastRun {
    pub job_id: JobId,
    pub state: JobState,
    pub started_ms: u64,
    pub checkpoint: BackendImportCheckpoint,
}

/// `None` before the first run and after the run's job record was pruned.
pub async fn last_run(
    context: &DriverContext,
    rule: &BackendImport,
) -> Result<Option<LastRun>, ImportRuleError> {
    let (Some(job_id), Some(started_ms)) = (rule.last_job, rule.last_started_ms) else {
        return Ok(None);
    };
    let Some(record) = read_job_record(&context.storage_handle, job_id, None)
        .await
        .map_err(ImportRuleError::Read)?
    else {
        return Ok(None);
    };
    let checkpoint = read_checkpoint(context, job_id)
        .await
        .map_err(ImportRuleError::Read)?
        .unwrap_or_default();
    Ok(Some(LastRun {
        job_id,
        state: record.state,
        started_ms,
        checkpoint,
    }))
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
    if !folder.ends_with('/') || folder.starts_with('/') || folder.contains("//") {
        return Err(invalid(
            "must be empty or a relative folder that ends with `/`",
        ));
    }
    ensure_confined_path(Path::new(folder)).map_err(|error| invalid(&error.to_string()))
}

/// Whether two folders of one bucket share keys.
pub fn folders_overlap(left: &str, right: &str) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

pub async fn list_imports(
    context: &DriverContext,
    group_id: GroupId,
) -> Result<Vec<BackendImport>, ImportRuleError> {
    scan_imports(context, Some(group_id.to_bytes().to_vec().into())).await
}

/// Every rule of every group on this node, for the schedule.
pub async fn all_imports(context: &DriverContext) -> Result<Vec<BackendImport>, ImportRuleError> {
    scan_imports(context, None).await
}

async fn scan_imports(
    context: &DriverContext,
    prefix: Option<Key>,
) -> Result<Vec<BackendImport>, ImportRuleError> {
    let mut rules = Vec::new();
    let mut start_after = None;
    loop {
        let (values, next) = iter_prefix_page(
            &context.storage_handle,
            BACKEND_IMPORT_KEYSPACE,
            prefix.clone(),
            start_after,
            RULE_PAGE,
            None,
        )
        .await
        .map_err(ImportRuleError::Read)?;
        for (_, value) in values {
            rules.push(BackendImport::from_bytes(value.as_ref())?);
        }
        match next {
            Some(next) => start_after = Some(next),
            None => return Ok(rules),
        }
    }
}

pub async fn read_import(
    context: &DriverContext,
    group_id: GroupId,
    import_id: Ulid,
) -> Result<Option<BackendImport>, ImportRuleError> {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
            key: import_key(group_id, import_id),
            txn_id: None,
        })
        .await;
    Ok(parse_read(event, BackendImport::from_bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        ] {
            assert!(validate_folder("folder", folder).is_err(), "{folder}");
        }
        assert!(folders_overlap("", "raw/"));
        assert!(folders_overlap("raw/x/", "raw/"));
        assert!(!folders_overlap("raw/", "rawer/"));
    }
}
