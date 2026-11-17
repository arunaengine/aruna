//! Reads every record of one job family completely, or reports why it could not.
//! A partial or failed read is an availability failure, never proof that a record is absent.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::FAMILY_RECORD_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    JobFamilyId, JobRecordEnvelope, JobRecordKey, JobRecordKind,
};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;

use super::keys::{family_prefix, kind_prefix};
use super::rows::from_bytes;
use super::{MAX_PROJECTION_RECORDS, RECORD_PAGE_SIZE};

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

/// Pages one family prefix to its end. A page error, an undecodable row, or a
/// continuation past the bound fails the whole read instead of returning the
/// part that happened to load.
#[derive(Debug, PartialEq)]
pub struct LoadFamilyOperation {
    prefix: Key,
    limit: usize,
    records: Vec<JobRecordEnvelope>,
    started: bool,
    outcome: Option<Result<Vec<JobRecordEnvelope>, FamilyReadError>>,
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
            started: false,
            outcome: None,
        }
    }

    fn page(&self, cursor: Option<Key>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: Some(self.prefix.clone()),
            start: cursor.map(IterStart::After),
            limit: RECORD_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn finish(&mut self, outcome: Result<Vec<JobRecordEnvelope>, FamilyReadError>) -> Effects {
        self.outcome = Some(outcome);
        smallvec![]
    }

    fn keep(
        &mut self,
        values: Vec<(Key, aruna_core::types::Value)>,
    ) -> Result<(), FamilyReadError> {
        for (key, value) in values {
            JobRecordKey::from_bytes(&key).map_err(|_| FamilyReadError::MalformedKey)?;
            self.records.push(from_bytes::<JobRecordEnvelope>(&value)?);
        }
        Ok(())
    }
}

impl Operation for LoadFamilyOperation {
    type Output = Vec<JobRecordEnvelope>;
    type Error = FamilyReadError;

    fn start(&mut self) -> Effects {
        self.started = true;
        self.page(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        if !self.started || self.outcome.is_some() {
            return self.finish(Err(FamilyReadError::Storage(format!(
                "unexpected storage event: {event:?}"
            ))));
        }
        match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                if let Err(error) = self.keep(values) {
                    return self.finish(Err(error));
                }
                match next_start_after {
                    Some(_) if self.records.len() >= self.limit => {
                        let loaded = self.records.len();
                        self.finish(Err(FamilyReadError::LimitExceeded { loaded }))
                    }
                    Some(next) => self.page(Some(next)),
                    None => {
                        let records = std::mem::take(&mut self.records);
                        self.finish(Ok(records))
                    }
                }
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.finish(Err(FamilyReadError::Storage(error.to_string())))
            }
            other => self.finish(Err(FamilyReadError::Storage(format!(
                "unexpected storage event: {other:?}"
            )))),
        }
    }

    fn is_complete(&self) -> bool {
        self.outcome.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(FamilyReadError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "family_tests.rs"]
mod state_machine_tests;
