//! Changes one backend import rule: its schedule, or the run it last started.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::ImportRuleError;
use super::schedule::parse_schedule;
use crate::groups::fence::{parse_group_record, read_group_record};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::BACKEND_IMPORT_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::storage::backend_import::{BackendImport, import_key};
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;
use std::time::SystemTime;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ImportChange {
    Schedule(Option<String>),
    Started { job_id: JobId, at_ms: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChangeState {
    Init,
    Start,
    Read,
    Write,
    Commit,
    Finish,
}

/// Rereads the rule in its own transaction, so a concurrent change is never lost.
#[derive(Debug, PartialEq)]
pub struct ChangeImportOperation {
    group_id: GroupId,
    import_id: Ulid,
    change: ImportChange,
    now: SystemTime,
    state: ChangeState,
    txn_id: Option<TxnId>,
    output: Option<Result<BackendImport, ImportRuleError>>,
}

impl ChangeImportOperation {
    pub fn new(group_id: GroupId, import_id: Ulid, change: ImportChange, now: SystemTime) -> Self {
        Self {
            group_id,
            import_id,
            change,
            now,
            state: ChangeState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, error: ImportRuleError) -> Effects {
        self.state = ChangeState::Finish;
        self.output = Some(Err(error));
        self.abort()
    }

    fn apply(&mut self, value: Option<aruna_core::types::Value>) -> Effects {
        let Some(value) = value else {
            return self.fail(ImportRuleError::NotFound);
        };
        let mut record = match BackendImport::from_bytes(&value) {
            Ok(record) => record,
            Err(error) => return self.fail(error.into()),
        };
        match &self.change {
            ImportChange::Schedule(schedule) => {
                record.schedule = schedule.clone();
                record.updated_at = self.now;
            }
            ImportChange::Started { job_id, at_ms } => {
                record.last_job = Some(*job_id);
                record.last_started_ms = Some(*at_ms);
            }
        }
        let value = match record.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(error.into()),
        };
        self.state = ChangeState::Write;
        let effect = Effect::Storage(StorageEffect::Write {
            key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
            key: record.key(),
            value: value.into(),
            txn_id: self.txn_id,
        });
        self.output = Some(Ok(record));
        smallvec![effect]
    }
}

impl Operation for ChangeImportOperation {
    type Output = BackendImport;
    type Error = ImportRuleError;

    fn start(&mut self) -> Effects {
        if let ImportChange::Schedule(Some(schedule)) = &self.change
            && let Err(error) = parse_schedule(schedule)
        {
            return self.fail(error);
        }
        self.state = ChangeState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error.into()),
            (ChangeState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = ChangeState::Read;
                let key = import_key(self.group_id, self.import_id);
                smallvec![read_group_record(
                    self.group_id,
                    BACKEND_IMPORT_KEYSPACE,
                    key,
                    txn_id
                )]
            }
            (ChangeState::Read, event) => match parse_group_record(event) {
                Ok(value) => self.apply(value),
                Err(error) => self.fail(error.into()),
            },
            (ChangeState::Write, Event::Storage(StorageEvent::WriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(ImportRuleError::Unexpected("Write"));
                };
                self.state = ChangeState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (ChangeState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = ChangeState::Finish;
                smallvec![]
            }
            _ => self.fail(ImportRuleError::Unexpected("ChangeImport")),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == ChangeState::Finish
    }

    fn finalize(self) -> Result<BackendImport, ImportRuleError> {
        self.output
            .unwrap_or(Err(ImportRuleError::Unexpected("Finish")))
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map(|txn_id| smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })])
            .unwrap_or_default()
    }
}
