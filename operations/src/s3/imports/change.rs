//! Changes the schedule and the write pattern of one backend import rule.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::ImportRuleError;
use super::cron::{is_running, parse_schedule};
use crate::groups::backends::backend_key;
use crate::groups::fence::group_fence_key;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, COMPRESSION_MIGRATION_KEYSPACE,
    IMPORT_DISCOVERY_KEYSPACE, IMPORT_PATTERN_KEYSPACE, JOB_KEYSPACE, S3_BUCKET_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::job::JobRecord;
use aruna_core::structs::execution::job::job_record_key;
use aruna_core::structs::identity::group_delete::check_group_write;
use aruna_core::structs::storage::backend_import::{
    BackendImport, import_key, object_prefix, read_patterns, retire_pattern, valid_pattern,
    write_patterns,
};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::discovery::{DiscoveryRules, check_naming};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::format::{Compression, CompressionMigration};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::types::{GroupId, Key, Value};
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
    Read,
    Run,
    Write,
}

type Step = Result<Next<BackendImport>, ImportRuleError>;

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
    /// The changed rule and the pattern it replaces, written once no run or move uses it.
    changed: Option<(BackendImport, String)>,
    /// The rule written; the write answer follows.
    output: Option<BackendImport>,
}

impl ChangeImportOperation {
    pub fn new(group_id: GroupId, import_id: Ulid, change: ImportChange, now: SystemTime) -> Self {
        Self {
            group_id,
            import_id,
            change,
            now,
            state: ChangeState::Read,
            changed: None,
            output: None,
        }
    }

    /// The rule row, refused while its group is being deleted.
    fn apply(&mut self, values: Vec<Option<Value>>) -> Step {
        let [value, fence] = <[_; 2]>::try_from(values).map_err(|_| IoError::Unexpected {
            expected: "the rule and its group fence",
            got: "a read of another length",
        })?;
        check_group_write(fence.as_deref())?;
        let value = value.ok_or(ImportRuleError::NotFound)?;
        let mut record = BackendImport::from_bytes(&value)?;
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
            (
                S3_BUCKET_KEYSPACE.to_string(),
                record.bucket.as_bytes().to_vec().into(),
            ),
            (
                BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                record.bucket.as_bytes().to_vec().into(),
            ),
            (IMPORT_DISCOVERY_KEYSPACE.to_string(), record.key()),
        ];
        if let Some(last_job) = record.last_job {
            reads.push((JOB_KEYSPACE.to_string(), job_record_key(last_job)));
        }
        self.changed = Some((record, before.write_pattern));
        self.state = ChangeState::Run;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// A move still running would publish files named by the old pattern; a pruned last run
    /// has ended. The old pattern stays to read the files it named.
    fn run_read(&mut self, values: Vec<Option<Value>>) -> Step {
        let Some((record, old)) = self.changed.take() else {
            return Err(ImportRuleError::Unexpected("Run"));
        };
        let mut values = values.into_iter();
        let (moving, earlier) = (values.next().flatten(), values.next().flatten());
        let (storage, bucket) = (values.next().flatten(), values.next().flatten());
        let (encryption, mappings) = (values.next().flatten(), values.next().flatten());
        let run = values.next().flatten();
        let rules = (mappings.as_deref())
            .map(DiscoveryRules::from_bytes)
            .transpose()?
            .unwrap_or_default();
        check_naming(&rules.rules, &record.write_pattern).map_err(ImportRuleError::Invalid)?;
        let storage = (storage.as_deref())
            .map(GroupStorage::from_bytes)
            .transpose()?
            .ok_or(ImportRuleError::NotFound)?;
        // A rule that becomes a 1:1 link needs a bucket that stores plain files.
        let mut old_rule = record.clone();
        old_rule.write_pattern = old.clone();
        // Without a shared bucket, encoded writes of an ordinary link would have nowhere to go.
        let shared = !storage.splits() || storage.default_bucket().is_some();
        if old_rule.mapped_bucket().is_some() && record.mapped_bucket().is_none() && !shared {
            return Err(ImportRuleError::Conflict(
                "Its storage has no shared bucket, so this link keeps {dir}{name}.".to_string(),
            ));
        }
        if record.mapped_bucket().is_some() && old_rule.mapped_bucket().is_none() {
            let info = bucket.as_deref().map(BucketInfo::from_bytes).transpose()?;
            let settings = BucketEncryption::from_row(encryption.as_deref())?;
            let compressed =
                (info.as_ref()).is_some_and(|info| info.compression != Compression::Off);
            if settings.is_encrypted() || compressed {
                return Err(ImportRuleError::Conflict(super::MAPPED_REASON.to_string()));
            }
        }
        let moving = moving.as_deref().map(CompressionMigration::from_bytes);
        if let Some(found) = moving.transpose()?
            && found.moving
            && found.finished_at_ms.is_none()
        {
            return Err(ImportRuleError::Conflict(
                "a move of this bucket has not finished".to_string(),
            ));
        }
        if let Some(last) = run.as_deref().map(JobRecord::from_bytes).transpose()?
            && is_running(last.state)
        {
            return Err(ImportRuleError::Conflict(
                "the last run of this import has not finished".to_string(),
            ));
        }
        let earlier = read_patterns(earlier.as_deref())?;
        let earlier = retire_pattern(&earlier, &old, &record.write_pattern);
        let value = write_patterns(&earlier)?.into();
        let key = object_prefix(record.import_id);
        self.write(record, Some((key, value)))
    }

    /// Writes the rule, with the earlier patterns of a pattern change in the same batch.
    fn write(&mut self, record: BackendImport, earlier: Option<(Key, Value)>) -> Step {
        let value = record.to_bytes()?.into();
        self.state = ChangeState::Write;
        let mut writes = vec![(BACKEND_IMPORT_KEYSPACE.to_string(), record.key(), value)];
        if let Some((patterns, earlier)) = earlier {
            writes.push((IMPORT_PATTERN_KEYSPACE.to_string(), patterns, earlier));
        }
        self.output = Some(record);
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for ChangeImportOperation {
    type Output = BackendImport;
    type Error = ImportRuleError;

    fn first(&mut self) -> Step {
        if let Some(schedule) = &self.change.schedule {
            parse_schedule(schedule)?;
        }
        if let Some(pattern) = &self.change.write_pattern
            && !valid_pattern(pattern)
        {
            return Err(ImportRuleError::Invalid(
                "write_pattern needs {name} and may only use {dir}, {name} and {id}".to_string(),
            ));
        }
        let key = import_key(self.group_id, self.import_id);
        let reads = vec![
            (BACKEND_IMPORT_KEYSPACE.to_string(), key),
            group_fence_key(self.group_id),
        ];
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.state {
            ChangeState::Read => self.apply(result.values()?),
            ChangeState::Run => self.run_read(result.values()?),
            ChangeState::Write => {
                result.done()?;
                let record = self
                    .output
                    .take()
                    .ok_or(ImportRuleError::Unexpected("Write"))?;
                Ok(Next::Done(record))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
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
    fn read(stored: &BackendImport, pattern: Option<&str>) -> (ChangeImportOperation, Step) {
        let change = ImportChange {
            schedule: None,
            write_pattern: pattern.map(str::to_string),
        };
        let (group_id, import_id) = (stored.group_id, stored.import_id);
        let mut operation =
            ChangeImportOperation::new(group_id, import_id, change, SystemTime::now());
        assert_eq!(operation.transaction(), TransactionMode::Write);
        let _ = operation.first();
        let values = vec![Some(stored.to_bytes().unwrap().into()), None];
        let turn = operation.next(IoResult::Values(values));
        (operation, turn)
    }

    fn writes(turn: &Step) -> &[(String, Key, Value)] {
        match turn {
            Ok(Next::Call(IoCall::Write(writes))) => writes,
            other => panic!("expected the rule write, got {other:?}"),
        }
    }

    fn written(turn: &Step) -> BackendImport {
        let writes = writes(turn);
        assert_eq!(writes[0].0, BACKEND_IMPORT_KEYSPACE);
        BackendImport::from_bytes(&writes[0].2).unwrap()
    }

    /// The earlier patterns a pattern change stores with the rule.
    fn earlier(turn: &Step) -> Vec<String> {
        let writes = writes(turn);
        let [_, (space, key, value)] = writes else {
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

    fn finish(mut operation: ChangeImportOperation) -> BackendImport {
        match operation.next(IoResult::Done) {
            Ok(Next::Done(record)) => record,
            other => panic!("expected the changed rule, got {other:?}"),
        }
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

    /// A storage without a fixed bucket that has a shared bucket.
    fn storage_with_bucket() -> Value {
        let mut record = GroupStorage::from_bytes(&storage(true)).unwrap();
        (record.public_config).insert("default_bucket".to_string(), "lab-shared".to_string());
        record.to_bytes().unwrap().into()
    }

    /// Answers the checks of a pattern change with a move record, the storage and the last run.
    fn check(
        operation: &mut ChangeImportOperation,
        turn: &Step,
        moving: Option<CompressionMigration>,
        run: Option<Value>,
    ) -> Step {
        check_on(operation, turn, (moving, None, None), storage(true), run)
    }

    /// Like `check`, with the bucket record that the switch to a 1:1 link reads.
    fn check_on(
        operation: &mut ChangeImportOperation,
        turn: &Step,
        (moving, bucket, mappings): (Option<CompressionMigration>, Option<Value>, Option<Value>),
        storage: Value,
        run: Option<Value>,
    ) -> Step {
        let Ok(Next::Call(IoCall::Read(reads))) = turn else {
            panic!("expected the checks, got {turn:?}")
        };
        let spaces: Vec<&str> = reads.iter().map(|(space, _)| space.as_str()).collect();
        let mut expected = vec![
            COMPRESSION_MIGRATION_KEYSPACE,
            IMPORT_PATTERN_KEYSPACE,
            STORAGE_BACKEND_KEYSPACE,
            S3_BUCKET_KEYSPACE,
            BUCKET_ENCRYPTION_KEYSPACE,
            IMPORT_DISCOVERY_KEYSPACE,
        ];
        if run.is_some() {
            expected.push(JOB_KEYSPACE);
        }
        assert_eq!(spaces, expected);
        let moving = moving.map(|record| record.to_bytes().unwrap().into());
        let answers = [moving, None, Some(storage), bucket, None, mappings, run];
        let values = answers.into_iter().take(reads.len()).collect();
        operation.next(IoResult::Values(values))
    }

    fn conflict(turn: &Step) {
        assert!(matches!(turn, Err(ImportRuleError::Conflict(_))));
    }

    #[test]
    fn changes_write_pattern() {
        // The rule keeps its id; the schedule follows the request as before.
        let stored = rule("{dir}{id}_{name}", None);
        let (mut operation, turn) = read(&stored, Some(DEFAULT_PATTERN));
        let turn = check(&mut operation, &turn, None, None);
        let changed = written(&turn);
        assert_eq!(changed.import_id, stored.import_id);
        assert_eq!(changed.write_pattern, DEFAULT_PATTERN);
        assert_eq!(changed.schedule, None);
        // The replaced pattern stays, so files it named keep their versions.
        assert_eq!(earlier(&turn), ["{dir}{id}_{name}"]);
        assert_eq!(finish(operation), changed);

        // Without a pattern only the schedule changes.
        let (_, turn) = read(&stored, None);
        assert_eq!(writes(&turn).len(), 1);
        assert_eq!(written(&turn).write_pattern, stored.write_pattern);
    }

    #[test]
    fn waits_for_run() {
        let stored = rule("{dir}{id}_{name}", Some(job_id()));
        for (state, ends) in [(JobState::Running, false), (JobState::Succeeded, true)] {
            let (mut operation, turn) = read(&stored, Some(DEFAULT_PATTERN));
            let turn = check(&mut operation, &turn, None, Some(job(state)));
            match ends {
                true => assert_eq!(written(&turn).write_pattern, DEFAULT_PATTERN),
                false => conflict(&turn),
            }
        }
        // The same pattern needs no check.
        let (_, turn) = read(&stored, Some("{dir}{id}_{name}"));
        written(&turn);
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
            let (mut operation, turn) = read(&stored, Some(DEFAULT_PATTERN));
            let turn = check(&mut operation, &turn, Some(record), None);
            match ends {
                true => assert_eq!(written(&turn).write_pattern, DEFAULT_PATTERN),
                false => conflict(&turn),
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
        assert!(matches!(
            operation.first(),
            Err(ImportRuleError::Invalid(_))
        ));
    }

    #[test]
    fn switches_whole_links() {
        // A whole-bucket link may switch its naming; one without `{id}` is a 1:1 link.
        let mut linked = rule(PLAIN_PATTERN, None);
        linked.backend_prefix = "lab/".to_string();
        linked.key_prefix = String::new();
        assert!(linked.mapped_bucket().is_some());
        let mut whole = linked.clone();
        whole.write_pattern = DEFAULT_PATTERN.to_string();
        assert!(whole.mapped_bucket().is_none());
        let shared = storage_with_bucket();
        for (stored, pattern) in [(&linked, DEFAULT_PATTERN), (&whole, "{dir}raw/{name}")] {
            let (mut operation, turn) = read(stored, Some(pattern));
            let checks = (None, None, None);
            let turn = check_on(&mut operation, &turn, checks, shared.clone(), None);
            assert_eq!(written(&turn).write_pattern, pattern);
        }
        // Without a shared bucket on its storage a 1:1 link keeps its naming.
        let (mut operation, turn) = read(&linked, Some(DEFAULT_PATTERN));
        conflict(&check(&mut operation, &turn, None, None));
        // A compressed bucket cannot become a 1:1 link.
        let info = BucketInfo {
            group_id: Ulid::from_parts(3, 3),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Zstd { level: 3 },
            distribution: None,
        };
        let bucket = Some(info.to_bytes().unwrap().into());
        let (mut operation, turn) = read(&whole, Some(PLAIN_PATTERN));
        let turn = check_on(
            &mut operation,
            &turn,
            (None, bucket, None),
            storage(true),
            None,
        );
        conflict(&turn);

        // A storage folder mapping keeps its folder only with `{dir}` in the naming.
        let mappings = DiscoveryRules {
            rules: vec![aruna_core::structs::storage::discovery::DiscoveryRule {
                pattern: "lars/{name}".to_string(),
                folder: "foobar/".to_string(),
            }],
        };
        let mappings = Some(mappings.to_bytes().unwrap().into());
        let (mut operation, turn) = read(&linked, Some("{name}~{id}"));
        let turn = check_on(
            &mut operation,
            &turn,
            (None, None, mappings),
            storage(true),
            None,
        );
        assert!(matches!(turn, Err(ImportRuleError::Invalid(_))));
    }
}
