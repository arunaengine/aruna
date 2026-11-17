//! Pages the immutable job record log in stable key order, including refused conflict rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, FetchCursor, PageLimit, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{FAMILY_CONFLICT_KEYSPACE, FAMILY_RECORD_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    JobFamilyId, JobRecordEnvelope, JobRecordKey, SubmissionId,
};
use aruna_core::types::{Key, Value};

use super::{MAX_CONFLICT_ROWS, RecordStoreError};
use aruna_core::structs::execution::job::record_keys::{
    cursor_key, cursor_of, family_prefix, record_key, submission_prefix,
};
use aruna_core::structs::execution::job::record_rows::{ConflictRecord, from_bytes};

/// Records of which scope one audit pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditScope {
    /// One request family: the canonical binding or one idempotency conflict.
    Family(JobFamilyId),
    /// Every request family of one submission, in key order.
    Submission(SubmissionId),
}

impl AuditScope {
    fn prefix(&self) -> Key {
        match self {
            AuditScope::Family(family) => family_prefix(family),
            AuditScope::Submission(submission_id) => submission_prefix(*submission_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FamilyAuditConfig {
    pub scope: AuditScope,
    /// Opaque cursor of the previous page; it carries one record key.
    pub cursor: Option<FetchCursor>,
    pub limit: PageLimit,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditPage {
    pub records: Vec<JobRecordEnvelope>,
    pub next: Option<FetchCursor>,
    /// Refused same-key/different-digest records, reported with the first page.
    pub conflicts: Vec<ConflictRecord>,
}

#[derive(Debug, PartialEq)]
pub struct FamilyAuditOperation {
    config: FamilyAuditConfig,
    records: Vec<JobRecordEnvelope>,
    last: Option<JobRecordKey>,
    state: AuditState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuditState {
    Page,
    Conflicts,
}

impl FamilyAuditOperation {
    pub fn new(config: FamilyAuditConfig) -> Self {
        Self {
            config,
            records: Vec::new(),
            last: None,
            state: AuditState::Page,
        }
    }

    fn keep_page(&mut self, values: Vec<(Key, Value)>) {
        for (key, value) in values {
            let Ok(record_key) = JobRecordKey::from_bytes(&key) else {
                continue;
            };
            self.last = Some(record_key);
            if let Ok(envelope) = from_bytes::<JobRecordEnvelope>(&value) {
                self.records.push(envelope);
            }
        }
    }

    fn settle(&mut self, conflicts: Vec<ConflictRecord>) -> Next<AuditPage> {
        // A short page is the end of the scope, so no cursor is minted for it.
        let next = match self.records.len() >= self.config.limit.get() {
            true => self.last.as_ref().and_then(|key| cursor_of(key).ok()),
            false => None,
        };
        Next::Done(AuditPage {
            records: std::mem::take(&mut self.records),
            next,
            conflicts,
        })
    }

    fn unexpected(&self, expected: &'static str, got: &'static str) -> RecordStoreError {
        RecordStoreError::unexpected(&self.state, expected, got)
    }
}

impl Operation for FamilyAuditOperation {
    type Output = AuditPage;
    type Error = RecordStoreError;

    fn first(&mut self) -> Result<Next<AuditPage>, RecordStoreError> {
        let after = match self.config.cursor.as_ref().map(cursor_key).transpose()? {
            Some(key) => Some(record_key(&key)),
            None => None,
        };
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: self.config.scope.prefix(),
            after,
            limit: self.config.limit.get(),
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AuditPage>, RecordStoreError> {
        match (self.state, result) {
            (AuditState::Page, IoResult::Page { rows, .. }) => {
                self.keep_page(rows);
                if self.config.cursor.is_some() {
                    return Ok(self.settle(Vec::new()));
                }
                self.state = AuditState::Conflicts;
                // A plain storage effect, so a failed conflict read comes back as an event.
                Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(
                    StorageEffect::Iter {
                        key_space: FAMILY_CONFLICT_KEYSPACE.to_string(),
                        prefix: Some(self.config.scope.prefix()),
                        start: None,
                        limit: MAX_CONFLICT_ROWS,
                        txn_id: None,
                    },
                )))))
            }
            (AuditState::Conflicts, IoResult::Event(event)) => match *event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    let conflicts = values
                        .into_iter()
                        .filter_map(|(_, value)| from_bytes::<ConflictRecord>(&value).ok())
                        .collect();
                    Ok(self.settle(conflicts))
                }
                // Conflict evidence is diagnostic: a failed read must not deny
                // the audit page itself.
                Event::Storage(StorageEvent::Error { .. }) => Ok(self.settle(Vec::new())),
                other => Err(self.unexpected("conflict page", other.label())),
            },
            (AuditState::Page, other) => Err(self.unexpected("record page", other.label())),
            (AuditState::Conflicts, other) => Err(self.unexpected("conflict page", other.label())),
        }
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::effects::{Effect, PageLimit};
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::execution::job::SubmissionId;

    use super::{AuditPage, AuditScope, FamilyAuditConfig, FamilyAuditOperation};
    use crate::jobs::records::RecordStoreError;

    fn empty_page() -> IoResult {
        IoResult::Page {
            rows: Vec::new(),
            next: None,
        }
    }

    fn operation() -> FamilyAuditOperation {
        FamilyAuditOperation::new(FamilyAuditConfig {
            scope: AuditScope::Submission(SubmissionId([3; 32])),
            cursor: None,
            limit: PageLimit::try_from(3).expect("bounded limit"),
        })
    }

    fn empty_audit() -> Next<AuditPage> {
        Next::Done(AuditPage {
            records: Vec::new(),
            next: None,
            conflicts: Vec::new(),
        })
    }

    #[test]
    fn keeps_page_without_conflicts() {
        let mut audit = operation();
        assert!(matches!(audit.first(), Ok(Next::Call(IoCall::Page { .. }))));
        assert!(matches!(
            audit.next(empty_page()),
            Ok(Next::Call(IoCall::Effect(effect))) if matches!(*effect, Effect::Storage(_))
        ));
        let failed = Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        });
        assert_eq!(audit.next(IoResult::Event(Box::new(failed))), Ok(empty_audit()));
    }

    #[test]
    fn rejects_unfit_result() {
        let mut audit = operation();
        audit.first().expect("record page");
        assert!(matches!(
            audit.next(IoResult::Done),
            Err(RecordStoreError::UnexpectedEvent { .. })
        ));
    }
}
