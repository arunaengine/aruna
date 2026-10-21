//! Changes the schedule and the write pattern of one backend import rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::ImportRuleError;
use super::schedule::{is_running, parse_schedule};
use crate::groups::backends::backend_key;
use crate::groups::fence::{parse_group_record, read_group_record};
use crate::jobs::store::decode_job_record;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, COMPRESSION_MIGRATION_KEYSPACE, IMPORT_PATTERN_KEYSPACE, JOB_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::job_record_key;
use aruna_core::structs::storage::backend_import::{
    BackendImport, import_key, object_prefix, read_patterns, retire_pattern, valid_pattern,
    write_patterns,
};
use aruna_core::structs::storage::format::CompressionMigration;
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
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
/// for the rule's last run and the bucket's move to end, so none uses the old pattern.
#[derive(Debug, PartialEq)]
pub struct ChangeImportOperation {
    group_id: GroupId,
    import_id: Ulid,
    change: ImportChange,
    now: SystemTime,
    state: ChangeState,
    txn_id: Option<TxnId>,
    /// The write carries the earlier patterns too, as one batch.
    batched: bool,
    /// The changed rule and the pattern it replaces, written once no run or move uses it.
    changed: Option<(BackendImport, String)>,
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
            batched: false,
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
            return self.write(record, None);
        };
        record.write_pattern = pattern;
        if record.write_pattern == before.write_pattern {
            return self.write(record, None);
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(ImportRuleError::Unexpected("Read"));
        };
        let mut reads = vec![
            (
                COMPRESSION_MIGRATION_KEYSPACE.to_string(),
                record.bucket.as_bytes().to_vec().into(),
            ),
            (
                IMPORT_PATTERN_KEYSPACE.to_string(),
                object_prefix(record.import_id),
            ),
            (
                STORAGE_BACKEND_KEYSPACE.to_string(),
                backend_key(record.backend_id),
            ),
        ];
        if let Some(last_job) = record.last_job {
            reads.push((JOB_KEYSPACE.to_string(), job_record_key(last_job)));
        }
        self.changed = Some((record, before.write_pattern));
        self.state = ChangeState::Run;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
    }

    /// A move still running would publish files named by the old pattern; a pruned last run
    /// has ended. The old pattern stays to read the files it named.
    fn run_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let Some((record, old)) = self.changed.take() else {
            return self.fail(ImportRuleError::Unexpected("Run"));
        };
        let mut values = values.into_iter().map(|(_, value)| value);
        let (moving, earlier) = (values.next().flatten(), values.next().flatten());
        let (storage, run) = (values.next().flatten(), values.next().flatten());
        let splits = match storage.as_deref().map(GroupStorage::from_bytes).transpose() {
            Ok(Some(storage)) => storage.splits(),
            Ok(None) => return self.fail(ImportRuleError::NotFound),
            Err(error) => return self.fail(error.into()),
        };
        // A linked provider bucket keeps its plain names, and no rule becomes one.
        let mut old_rule = record.clone();
        old_rule.write_pattern = old.clone();
        if splits && record.mapped_bucket() != old_rule.mapped_bucket() {
            return self.fail(ImportRuleError::Invalid(
                "a linked provider bucket keeps the plain pattern".to_string(),
            ));
        }
        let moving = moving.as_deref().map(CompressionMigration::from_bytes);
        match moving.transpose() {
            Ok(Some(found)) if found.moving && found.finished_at_ms.is_none() => {
                return self.fail(ImportRuleError::Conflict(
                    "a move of this bucket has not finished".to_string(),
                ));
            }
            Ok(_) => {}
            Err(error) => return self.fail(error.into()),
        }
        match run.as_deref().map(decode_job_record).transpose() {
            Ok(Some(last)) if is_running(last.state) => {
                return self.fail(ImportRuleError::Conflict(
                    "the last run of this import has not finished".to_string(),
                ));
            }
            Ok(_) => {}
            Err(error) => return self.fail(error.into()),
        }
        let earlier = match read_patterns(earlier.as_deref()) {
            Ok(earlier) => retire_pattern(&earlier, &old, &record.write_pattern),
            Err(error) => return self.fail(error.into()),
        };
        let (key, value) = match write_patterns(&earlier) {
            Ok(value) => (object_prefix(record.import_id), value.into()),
            Err(error) => return self.fail(error.into()),
        };
        self.write(record, Some((key, value)))
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(ImportRuleError::Unexpected("Write"));
        };
        self.state = ChangeState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    /// Writes the rule, with the earlier patterns of a pattern change in the same batch.
    fn write(&mut self, record: BackendImport, earlier: Option<(Key, Value)>) -> Effects {
        let value = match record.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(error.into()),
        };
        self.state = ChangeState::Write;
        self.batched = earlier.is_some();
        let (key, value) = (record.key(), value.into());
        let effect = match earlier {
            None => StorageEffect::Write {
                key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
                key,
                value,
                txn_id: self.txn_id,
            },
            Some((patterns, earlier)) => StorageEffect::BatchWrite {
                writes: vec![
                    (BACKEND_IMPORT_KEYSPACE.to_string(), key, value),
                    (IMPORT_PATTERN_KEYSPACE.to_string(), patterns, earlier),
                ],
                txn_id: self.txn_id,
            },
        };
        self.output = Some(Ok(record));
        smallvec![Effect::Storage(effect)]
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
            (ChangeState::Run, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.run_read(values)
            }
            // A pattern change writes a batch, any other change one record.
            (ChangeState::Write, Event::Storage(StorageEvent::WriteResult { .. }))
                if !self.batched =>
            {
                self.commit()
            }
            (ChangeState::Write, Event::Storage(StorageEvent::BatchWriteResult { .. }))
                if self.batched =>
            {
                self.commit()
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
        match effects.as_slice() {
            [Effect::Storage(StorageEffect::Write { value, .. })] => {
                BackendImport::from_bytes(value).unwrap()
            }
            [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] => {
                BackendImport::from_bytes(&writes[0].2).unwrap()
            }
            _ => panic!("expected the rule write, got {effects:?}"),
        }
    }

    /// The earlier patterns a pattern change stores with the rule.
    fn earlier(effects: &Effects) -> Vec<String> {
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected the rule and its patterns, got {effects:?}")
        };
        let [_, (space, key, value)] = writes.as_slice() else {
            panic!("expected two writes, got {writes:?}")
        };
        assert_eq!(
            (space.as_str(), key),
            (
                IMPORT_PATTERN_KEYSPACE,
                &object_prefix(Ulid::from_parts(2, 2))
            )
        );
        read_patterns(Some(value)).unwrap()
    }

    fn finish(mut operation: ChangeImportOperation, key: Key) -> BackendImport {
        let written = |batched: bool| match batched {
            true => StorageEvent::BatchWriteResult {
                entries: Vec::new(),
            },
            false => StorageEvent::WriteResult { key: key.clone() },
        };
        // A pattern change's batch answers as one; another answer would not count.
        let change = operation.change.clone();
        let mut wrong =
            ChangeImportOperation::new(Ulid::nil(), Ulid::nil(), change, SystemTime::UNIX_EPOCH);
        (wrong.state, wrong.txn_id, wrong.batched) = (ChangeState::Write, Some(TXN), true);
        wrong.step(Event::Storage(written(false)));
        assert!(matches!(
            wrong.output,
            Some(Err(ImportRuleError::Unexpected(_)))
        ));
        let written = written(operation.batched);
        operation.step(Event::Storage(written));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TXN,
        }));
        operation.finalize().unwrap()
    }

    /// An S3 storage; without a fixed bucket its first folder names a provider bucket.
    fn storage(splits: bool) -> Value {
        let bucket = (!splits).then(|| ("bucket".to_string(), "lab".to_string()));
        let storage = GroupStorage {
            backend_id: Ulid::from_parts(4, 4),
            group_id: Ulid::from_parts(3, 3),
            name: "tenant".to_string(),
            kind: aruna_core::structs::storage::group_backend::GroupBackendKind::S3,
            public_config: bucket.into_iter().collect(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: false,
            cleanup: Default::default(),
        };
        assert_eq!(storage.splits(), splits);
        storage.to_bytes().unwrap().into()
    }

    /// Answers the checks of a pattern change with a move record, the storage and the last run.
    fn check(
        operation: &mut ChangeImportOperation,
        effects: &Effects,
        moving: Option<CompressionMigration>,
        run: Option<Value>,
    ) -> Effects {
        check_on(operation, effects, moving, storage(true), run)
    }

    fn check_on(
        operation: &mut ChangeImportOperation,
        effects: &Effects,
        moving: Option<CompressionMigration>,
        storage: Value,
        run: Option<Value>,
    ) -> Effects {
        let [Effect::Storage(StorageEffect::BatchRead { reads, .. })] = effects.as_slice() else {
            panic!("expected the checks, got {effects:?}")
        };
        let spaces: Vec<&str> = reads.iter().map(|(space, _)| space.as_str()).collect();
        let mut expected = vec![
            COMPRESSION_MIGRATION_KEYSPACE,
            IMPORT_PATTERN_KEYSPACE,
            STORAGE_BACKEND_KEYSPACE,
        ];
        if run.is_some() {
            expected.push(JOB_KEYSPACE);
        }
        assert_eq!(spaces, expected);
        let moving = moving.map(|record| record.to_bytes().unwrap().into());
        let answers = [moving, None, Some(storage), run];
        let values = reads
            .iter()
            .zip(answers)
            .map(|((_, key), value)| (key.clone(), value));
        let values = values.collect();
        operation.step(Event::Storage(StorageEvent::BatchReadResult { values }))
    }

    fn conflict(operation: ChangeImportOperation, effects: &Effects) {
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

    #[test]
    fn changes_write_pattern() {
        // The rule keeps its id; the schedule follows the request as before.
        let stored = rule("{dir}{id}_{name}", None);
        let (mut operation, effects) = read(&stored, Some(DEFAULT_PATTERN));
        let effects = check(&mut operation, &effects, None, None);
        let changed = written(&effects);
        assert_eq!(changed.import_id, stored.import_id);
        assert_eq!(changed.write_pattern, DEFAULT_PATTERN);
        assert_eq!(changed.schedule, None);
        // The replaced pattern stays, so files it named keep their versions.
        assert_eq!(earlier(&effects), ["{dir}{id}_{name}"]);
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
            let effects = check(&mut operation, &effects, None, Some(job(state)));
            match ends {
                true => assert_eq!(written(&effects).write_pattern, DEFAULT_PATTERN),
                false => conflict(operation, &effects),
            }
        }
        // The same pattern needs no check.
        let (_, effects) = read(&stored, Some("{dir}{id}_{name}"));
        written(&effects);
    }

    #[test]
    fn waits_for_move() {
        // A running move may still publish a file it tagged under the old pattern.
        let stored = rule(PLAIN_PATTERN, None);
        let mut moving =
            CompressionMigration::new(aruna_core::structs::storage::format::Compression::Off, 1);
        moving.moving = true;
        let mut done = moving.clone();
        done.finished_at_ms = Some(2);
        let mut encoding = moving.clone();
        encoding.moving = false;
        for (record, ends) in [(moving, false), (done, true), (encoding, true)] {
            let (mut operation, effects) = read(&stored, Some(DEFAULT_PATTERN));
            let effects = check(&mut operation, &effects, Some(record), None);
            match ends {
                true => assert_eq!(written(&effects).write_pattern, DEFAULT_PATTERN),
                false => conflict(operation, &effects),
            }
        }
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
            let (mut operation, effects) = read(stored, Some(pattern));
            check(&mut operation, &effects, None, None);
            assert!(matches!(
                operation.finalize(),
                Err(ImportRuleError::Invalid(_))
            ));
            // On a storage with a fixed bucket the same rule is an ordinary folder link.
            let (mut operation, effects) = read(stored, Some(pattern));
            let effects = check_on(&mut operation, &effects, None, storage(false), None);
            assert_eq!(written(&effects).write_pattern, pattern);
        }
        let (_, effects) = read(&linked, Some(PLAIN_PATTERN));
        written(&effects);
    }
}
