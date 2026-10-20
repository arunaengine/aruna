//! Changes the schedule and the write pattern of one backend import rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::ImportRuleError;
use super::schedule::{is_running, parse_schedule};
use crate::groups::fence::{parse_group_record, read_group_record};
use crate::jobs::store::decode_job_record;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BACKEND_IMPORT_KEYSPACE, JOB_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::job_record_key;
use aruna_core::structs::storage::backend_import::{BackendImport, import_key, valid_pattern};
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;
use std::time::SystemTime;
use ulid::Ulid;

/// The new schedule, and a new write pattern unless `None`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportChange {
    pub schedule: Option<String>,
    pub write_pattern: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChangeState {
    Init,
    Start,
    Read,
    Run,
    Write,
    Commit,
    Finish,
}

/// Rereads the rule in its own transaction, so a concurrent change is never lost. A new
/// pattern names only later writes; files and their records stay where they are. It waits
/// until the rule's last run ended, so no run reads paths with the old pattern.
#[derive(Debug, PartialEq)]
pub struct ChangeImportOperation {
    group_id: GroupId,
    import_id: Ulid,
    change: ImportChange,
    now: SystemTime,
    state: ChangeState,
    txn_id: Option<TxnId>,
    /// The changed rule, written once the last run is known to have ended.
    changed: Option<BackendImport>,
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
            changed: None,
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
        let before = record.clone();
        record.schedule = self.change.schedule.clone();
        record.updated_at = self.now;
        let Some(pattern) = self.change.write_pattern.clone() else {
            return self.write(record);
        };
        record.write_pattern = pattern;
        // A whole-bucket link keeps its plain names, and no rule becomes one.
        if record.mapped_bucket() != before.mapped_bucket() {
            return self.fail(ImportRuleError::Invalid(
                "a linked provider bucket keeps the plain pattern".to_string(),
            ));
        }
        if record.write_pattern == before.write_pattern {
            return self.write(record);
        }
        let (Some(last_job), Some(txn_id)) = (record.last_job, self.txn_id) else {
            return self.write(record);
        };
        self.changed = Some(record);
        self.state = ChangeState::Run;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: JOB_KEYSPACE.to_string(),
            key: job_record_key(last_job),
            txn_id: Some(txn_id),
        })]
    }

    /// A pruned last run has ended.
    fn run_read(&mut self, value: Option<aruna_core::types::Value>) -> Effects {
        let Some(record) = self.changed.take() else {
            return self.fail(ImportRuleError::Unexpected("Run"));
        };
        match value.as_deref().map(decode_job_record).transpose() {
            Ok(Some(last)) if is_running(last.state) => self.fail(ImportRuleError::Conflict(
                "the last run of this import has not finished".to_string(),
            )),
            Ok(_) => self.write(record),
            Err(error) => self.fail(error.into()),
        }
    }

    fn write(&mut self, record: BackendImport) -> Effects {
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
        if let Some(schedule) = &self.change.schedule
            && let Err(error) = parse_schedule(schedule)
        {
            return self.fail(error);
        }
        if let Some(pattern) = &self.change.write_pattern
            && !valid_pattern(pattern)
        {
            return self.fail(ImportRuleError::Invalid(
                "write_pattern needs {name} and may only use {dir}, {name} and {id}".to_string(),
            ));
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
            (ChangeState::Run, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.run_read(value)
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::execution::job::{JobId, JobPayload, JobRecord, JobState};
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::backend_import::{
        BackendImportSpec, DEFAULT_PATTERN, PLAIN_PATTERN,
    };
    use aruna_core::types::{Key, Value};

    const TXN: TxnId = TxnId::from_parts(7, 7);

    fn job_id() -> JobId {
        JobId::from_bytes([9u8; 16])
    }

    fn rule(pattern: &str, last_job: Option<JobId>) -> BackendImport {
        BackendImport {
            import_id: Ulid::from_parts(2, 2),
            group_id: Ulid::from_parts(3, 3),
            backend_id: Ulid::from_parts(4, 4),
            backend_prefix: "raw/".to_string(),
            bucket: "bucket".to_string(),
            key_prefix: "in/".to_string(),
            schedule: Some("0 3 * * *".to_string()),
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: pattern.to_string(),
        }
    }

    fn job(state: JobState) -> Value {
        let spec = BackendImportSpec {
            auth_context: AuthContext {
                user_id: Default::default(),
                realm_id: RealmId([1u8; 32]),
                path_restrictions: None,
                session: None,
            },
            node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
            group_id: Ulid::from_parts(3, 3),
            import_id: Ulid::from_parts(2, 2),
        };
        let payload = JobPayload::BackendImport(spec.clone());
        let mut record = JobRecord::new(
            job_id(),
            payload,
            Default::default(),
            spec.node_id,
            1,
            1,
            None,
        );
        record.state = state;
        record.to_bytes().unwrap().into()
    }

    /// Runs the operation up to the rule read and answers it with `stored`.
    fn read(stored: &BackendImport, pattern: Option<&str>) -> (ChangeImportOperation, Effects) {
        let change = ImportChange {
            schedule: None,
            write_pattern: pattern.map(str::to_string),
        };
        let (group_id, import_id) = (stored.group_id, stored.import_id);
        let mut operation =
            ChangeImportOperation::new(group_id, import_id, change, SystemTime::now());
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TXN,
        }));
        let values = vec![
            (stored.key(), Some(stored.to_bytes().unwrap().into())),
            (Key::from(vec![0u8]), None),
        ];
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
        (operation, effects)
    }

    fn written(effects: &Effects) -> BackendImport {
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected the rule write, got {effects:?}")
        };
        BackendImport::from_bytes(value).unwrap()
    }

    fn finish(mut operation: ChangeImportOperation, key: Key) -> BackendImport {
        operation.step(Event::Storage(StorageEvent::WriteResult { key }));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TXN,
        }));
        operation.finalize().unwrap()
    }

    #[test]
    fn changes_write_pattern() {
        // The rule keeps its id; the schedule follows the request as before.
        let stored = rule("{dir}{id}_{name}", None);
        let (operation, effects) = read(&stored, Some(DEFAULT_PATTERN));
        let changed = written(&effects);
        assert_eq!(changed.import_id, stored.import_id);
        assert_eq!(changed.write_pattern, DEFAULT_PATTERN);
        assert_eq!(changed.schedule, None);
        assert_eq!(finish(operation, stored.key()), changed);

        // Without a pattern only the schedule changes.
        let (_, effects) = read(&stored, None);
        assert_eq!(written(&effects).write_pattern, stored.write_pattern);
    }

    #[test]
    fn waits_for_run() {
        let stored = rule("{dir}{id}_{name}", Some(job_id()));
        for (state, ends) in [(JobState::Running, false), (JobState::Succeeded, true)] {
            let (mut operation, effects) = read(&stored, Some(DEFAULT_PATTERN));
            let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice()
            else {
                panic!("expected the run read, got {effects:?}")
            };
            assert_eq!(key_space, JOB_KEYSPACE);
            let key = job_record_key(job_id());
            let value = Some(job(state));
            let effects = operation.step(Event::Storage(StorageEvent::ReadResult { key, value }));
            if ends {
                assert_eq!(written(&effects).write_pattern, DEFAULT_PATTERN);
                continue;
            }
            assert_eq!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::AbortTransaction {
                    txn_id: TXN
                })]
            );
            assert!(matches!(
                operation.finalize(),
                Err(ImportRuleError::Conflict(_))
            ));
        }
        // The same pattern needs no check.
        let (_, effects) = read(&stored, Some("{dir}{id}_{name}"));
        written(&effects);
    }

    #[test]
    fn refuses_bad_patterns() {
        let change = ImportChange {
            schedule: None,
            write_pattern: Some("{dir}{id}".to_string()),
        };
        let mut operation =
            ChangeImportOperation::new(Ulid::nil(), Ulid::nil(), change, SystemTime::now());
        assert!(operation.start().is_empty());
        assert!(matches!(
            operation.finalize(),
            Err(ImportRuleError::Invalid(_))
        ));

        // A linked provider bucket keeps plain names, and no rule turns into one.
        let mut linked = rule(PLAIN_PATTERN, None);
        linked.backend_prefix = "lab/".to_string();
        linked.key_prefix = String::new();
        assert!(linked.mapped_bucket().is_some());
        let mut whole = linked.clone();
        whole.write_pattern = DEFAULT_PATTERN.to_string();
        for (stored, pattern) in [(&linked, DEFAULT_PATTERN), (&whole, PLAIN_PATTERN)] {
            let (operation, _) = read(stored, Some(pattern));
            assert!(matches!(
                operation.finalize(),
                Err(ImportRuleError::Invalid(_))
            ));
        }
        let (_, effects) = read(&linked, Some(PLAIN_PATTERN));
        written(&effects);
    }
}
