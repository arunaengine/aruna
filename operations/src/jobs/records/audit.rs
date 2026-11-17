//! Pages the immutable job record log in stable key order, including refused conflict rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, FetchCursor, IterStart, PageLimit, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{FAMILY_CONFLICT_KEYSPACE, FAMILY_RECORD_KEYSPACE};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::execution::job::{
    JobFamilyId, JobRecordEnvelope, JobRecordKey, SubmissionId,
};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;

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
    outcome: Option<Result<AuditPage, RecordStoreError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuditState {
    Init,
    Page,
    Conflicts,
    Finish,
    Error,
}

impl FamilyAuditOperation {
    pub fn new(config: FamilyAuditConfig) -> Self {
        Self {
            config,
            records: Vec::new(),
            last: None,
            state: AuditState::Init,
            outcome: None,
        }
    }

    fn keep_page(&mut self, values: Vec<(Key, aruna_core::types::Value)>) {
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

    fn settle(&mut self, conflicts: Vec<ConflictRecord>) -> Effects {
        // A short page is the end of the scope, so no cursor is minted for it.
        let next = match self.records.len() >= self.config.limit.get() {
            true => self.last.as_ref().and_then(|key| cursor_of(key).ok()),
            false => None,
        };
        self.outcome = Some(Ok(AuditPage {
            records: std::mem::take(&mut self.records),
            next,
            conflicts,
        }));
        self.state = AuditState::Finish;
        smallvec![]
    }

    fn fail(&mut self, error: RecordStoreError) -> Effects {
        self.outcome = Some(Err(error));
        self.state = AuditState::Error;
        smallvec![]
    }

    fn unexpected(&mut self, expected: &'static str, got: &'static str) -> Effects {
        self.fail(RecordStoreError::unexpected(&self.state, expected, got))
    }
}

impl EffectOperation for FamilyAuditOperation {
    type Output = AuditPage;
    type Error = RecordStoreError;

    fn start(&mut self) -> Effects {
        let start = match self.config.cursor.as_ref().map(cursor_key) {
            Some(Ok(key)) => Some(IterStart::After(record_key(&key))),
            Some(Err(error)) => return self.fail(error.into()),
            None => None,
        };
        self.state = AuditState::Page;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: Some(self.config.scope.prefix()),
            start,
            limit: self.config.limit.get(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            AuditState::Page => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    self.keep_page(values);
                    if self.config.cursor.is_some() {
                        return self.settle(Vec::new());
                    }
                    self.state = AuditState::Conflicts;
                    smallvec![Effect::Storage(StorageEffect::Iter {
                        key_space: FAMILY_CONFLICT_KEYSPACE.to_string(),
                        prefix: Some(self.config.scope.prefix()),
                        start: None,
                        limit: MAX_CONFLICT_ROWS,
                        txn_id: None,
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("record page", other.label()),
            },
            AuditState::Conflicts => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    let conflicts = values
                        .into_iter()
                        .filter_map(|(_, value)| from_bytes::<ConflictRecord>(&value).ok())
                        .collect();
                    self.settle(conflicts)
                }
                // Conflict evidence is diagnostic: a failed read must not deny
                // the audit page itself.
                Event::Storage(StorageEvent::Error { .. }) => self.settle(Vec::new()),
                other => self.unexpected("conflict page", other.label()),
            },
            AuditState::Init | AuditState::Finish | AuditState::Error => {
                self.unexpected("no event in a terminal state", event.label())
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, AuditState::Finish | AuditState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(RecordStoreError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::effects::PageLimit;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::EffectOperation;
    use aruna_core::structs::execution::job::SubmissionId;

    use super::{AuditScope, FamilyAuditConfig, FamilyAuditOperation};
    use crate::jobs::records::RecordStoreError;

    fn empty_page() -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        })
    }

    fn operation() -> FamilyAuditOperation {
        FamilyAuditOperation::new(FamilyAuditConfig {
            scope: AuditScope::Submission(SubmissionId([3; 32])),
            cursor: None,
            limit: PageLimit::try_from(3).expect("bounded limit"),
        })
    }

    // A state that expects no event rejects one instead of ignoring it.
    #[test]
    fn rejects_terminal_event() {
        let mut early = operation();
        assert!(early.step(empty_page()).is_empty());
        assert!(matches!(
            early.finalize(),
            Err(RecordStoreError::UnexpectedEvent { .. })
        ));

        let mut audit = operation();
        audit.start();
        audit.step(empty_page());
        audit.step(empty_page());
        assert!(audit.is_complete());
        assert!(audit.step(empty_page()).is_empty());
        assert!(matches!(
            audit.finalize(),
            Err(RecordStoreError::UnexpectedEvent { .. })
        ));
    }
}
