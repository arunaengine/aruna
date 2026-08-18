//! Paginated audit of the immutable log.
//!
//! Pages are ordered by the stable record key, never by arrival, so a cursor is
//! a position in the log rather than a snapshot of one responder. Every claim,
//! budget, launch, receipt, update, output, and cancellation of the scope is
//! returned, together with the conflict rows that were refused under a key.

use aruna_core::effects::{Effect, FetchCursor, IterStart, PageLimit, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{JOB_FAMILY_CONFLICT_KEYSPACE, JOB_FAMILY_RECORD_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{JobFamilyId, JobRecordEnvelope, JobRecordKey, SubmissionId};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;

use super::keys::{cursor_key, cursor_of, family_prefix, record_key, submission_prefix};
use super::rows::{ConflictRecord, from_bytes};
use super::{MAX_CONFLICT_ROWS, RecordStoreError};

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

    fn unexpected(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(RecordStoreError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }
}

impl Operation for FamilyAuditOperation {
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
            key_space: JOB_FAMILY_RECORD_KEYSPACE.to_string(),
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
                        key_space: JOB_FAMILY_CONFLICT_KEYSPACE.to_string(),
                        prefix: Some(self.config.scope.prefix()),
                        start: None,
                        limit: MAX_CONFLICT_ROWS,
                        txn_id: None,
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("record page", format!("{other:?}")),
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
                other => self.unexpected("conflict page", format!("{other:?}")),
            },
            AuditState::Init | AuditState::Finish | AuditState::Error => smallvec![],
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
