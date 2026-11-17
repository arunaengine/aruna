//! Reads every record of one job family completely, or reports why it could not.
//! A partial or failed read is an availability failure, never proof that a record is absent.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::FAMILY_RECORD_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    JobFamilyId, JobRecordEnvelope, JobRecordKey, JobRecordKind,
};
use aruna_core::types::Key;
use thiserror::Error;

use super::{MAX_PROJECTION_RECORDS, RECORD_PAGE_SIZE};
use aruna_core::structs::execution::job::record_keys::{family_prefix, kind_prefix};
use aruna_core::structs::execution::job::record_rows::from_bytes;

/// Why one family could not be read completely. Every variant leaves the caller
/// undecided; none of them means the family is empty.
#[derive(Debug, PartialEq, Error)]
pub enum FamilyReadError {
    #[error("job family page read failed: {0}")]
    Storage(String),
    #[error(transparent)]
    Decode(#[from] ConversionError),
    /// A row under the prefix is not addressed by a record key, so the scan
    /// cannot tell what it holds or whether it belongs to this family.
    #[error("job family row key is malformed")]
    MalformedKey,
    #[error("job family exceeds the bounded read after {loaded} records")]
    LimitExceeded { loaded: usize },
    #[error("job family read did not finish")]
    NotFinished,
}

impl From<IoError> for FamilyReadError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished => Self::NotFinished,
            IoError::Storage(error) => Self::Storage(error.to_string()),
            error => Self::Storage(error.to_string()),
        }
    }
}

/// Pages one family prefix to its end. A page error, an undecodable row, or a
/// continuation past the bound fails the whole read instead of returning the
/// part that happened to load.
#[derive(Debug, PartialEq)]
pub struct LoadFamilyOperation {
    prefix: Key,
    limit: usize,
    records: Vec<JobRecordEnvelope>,
}

impl LoadFamilyOperation {
    /// Every record of one family.
    pub fn family(family: JobFamilyId) -> Self {
        Self::new(family_prefix(&family), MAX_PROJECTION_RECORDS)
    }

    /// Every record of one kind inside one family. An exact question reads its
    /// own kind prefix, so unrelated history can never hide a receipt, a
    /// cancellation, or an update behind the read bound.
    pub fn kind(family: JobFamilyId, kind: JobRecordKind) -> Self {
        Self::new(kind_prefix(&family, kind), MAX_PROJECTION_RECORDS)
    }

    fn new(prefix: Key, limit: usize) -> Self {
        Self {
            prefix,
            limit,
            records: Vec::new(),
        }
    }

    fn page<T>(&self, after: Option<Key>) -> Next<T> {
        Next::Call(IoCall::Page {
            space: FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: self.prefix.clone(),
            after,
            limit: RECORD_PAGE_SIZE,
        })
    }
}

impl Operation for LoadFamilyOperation {
    type Output = Vec<JobRecordEnvelope>;
    type Error = FamilyReadError;

    fn first(&mut self) -> Result<Next<Self::Output>, FamilyReadError> {
        Ok(self.page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, FamilyReadError> {
        let IoResult::Page { rows, next } = result else {
            return Err(FamilyReadError::Storage(
                "unexpected answer to a family page".to_string(),
            ));
        };
        for (key, value) in rows {
            JobRecordKey::from_bytes(&key).map_err(|_| FamilyReadError::MalformedKey)?;
            self.records.push(from_bytes::<JobRecordEnvelope>(&value)?);
        }
        match next {
            Some(_) if self.records.len() >= self.limit => Err(FamilyReadError::LimitExceeded {
                loaded: self.records.len(),
            }),
            Some(after) => Ok(self.page(Some(after))),
            None => Ok(Next::Done(std::mem::take(&mut self.records))),
        }
    }
}

#[cfg(test)]
#[path = "family_tests.rs"]
mod state_machine_tests;
