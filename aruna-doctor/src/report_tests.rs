//! Checks the node report sections and the job record counts on a database with known rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{JobRecords, build_report};
use aruna_core::keyspaces::{
    FAMILY_OUTBOX_KEYSPACE, FAMILY_PENDING_KEYSPACE, FAMILY_RECORD_KEYSPACE, JOB_KEYSPACE,
    PLUGIN_RUNS_KEYSPACE, PLUGIN_WORK_KEYSPACE, SCHEDULE_INDEX_KEYSPACE,
};
use aruna_core::structs::execution::job::record_rows::{OutboxEntry, PendingNeed, PendingRecord};
use aruna_core::structs::execution::job::{
    CollisionPolicy, ExecutionSpec, JobFamilyId, JobFamilyRecord, JobId, JobPayload, JobRecord,
    JobRecordEnvelope, JobState, PluginWorkAction, PluginWorkSpec, SubmissionClaim, SubmissionId,
    cleanup_job_id, due_index_key, job_prune_key, job_record_key,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::{NodeId, UserId};
use aruna_operations::plugins::work::{
    Binding, Identity, WorkKind, WorkRow, WorkState, index_key, row_key,
};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase};
use std::collections::BTreeMap;
use std::path::Path;
use tempfile::tempdir;
use ulid::Ulid;

const NOW_MS: u64 = 10 * 24 * 3_600_000;

fn realm() -> RealmId {
    RealmId::from_bytes([2u8; 32])
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[6u8; 32]).public()
}

fn user() -> UserId {
    UserId::local(Ulid::from_bytes([8u8; 16]), realm())
}

/// A job id with a valid placement handle, as the other job tests build them.
fn job_id(seed: u8) -> JobId {
    JobId::from_bytes([seed + 3; 16])
}

fn job(id: JobId, payload: JobPayload, state: JobState) -> JobRecord {
    let mut record = JobRecord::new(id, payload, user(), node(), 1_000, 1_000, None);
    record.state = state;
    record
}

fn execution() -> JobPayload {
    JobPayload::Execution(ExecutionSpec {
        group_id: Ulid::from_bytes([7u8; 16]),
        name: None,
        description: None,
        tags: BTreeMap::new(),
        image: "image".to_string(),
        entrypoint: None,
        command: Vec::new(),
        workdir: None,
        env: BTreeMap::new(),
        resources: Default::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: CollisionPolicy::default(),
    })
}

fn work_spec(install_id: Ulid, work_id: Ulid) -> JobPayload {
    JobPayload::PluginWork(PluginWorkSpec {
        realm_id: realm(),
        install_id,
        work_id,
        group_id: Ulid::from_bytes([7u8; 16]),
        action: PluginWorkAction::Copy {
            source: "a".to_string(),
            source_version: None,
            target: "b".to_string(),
            version_id: Ulid::from_bytes([3u8; 16]),
        },
    })
}

fn work_row(work_id: Ulid, state: WorkState) -> WorkRow {
    WorkRow {
        account: user(),
        work_id,
        kind: WorkKind::Copy,
        source: None,
        target: None,
        digest: [0u8; 32],
        binding: Binding {
            digest: String::new(),
            revision: 1,
            recipe: None,
        },
        context: None,
        run_id: Ulid::from_bytes([4u8; 16]),
        version_id: None,
        job: None,
        state,
        boundary: None,
        summary: None,
        notified: false,
        cancel: false,
        created_ms: 1_000,
        settled_ms: None,
    }
}

fn claim() -> JobRecordEnvelope {
    let family = JobFamilyId {
        submission_id: SubmissionId([9u8; 32]),
        request_digest: [8u8; 32],
    };
    let record = JobFamilyRecord::Claim(SubmissionClaim {
        submission_id: family.submission_id,
        job_id: job_id(9),
        request_digest: family.request_digest,
        spec_digest: [7u8; 32],
        committing_node_id: node(),
        accepted_at_ms: 11,
    });
    JobRecordEnvelope::sign(realm(), record, &iroh::SecretKey::from_bytes(&[6u8; 32])).unwrap()
}

/// Writes one known row set: an overdue execution held by its cleanup, an overdue plugin work
/// held by its unsettled row, a free overdue job, a future prune row, a due row, a family
/// record, two pending rows and two outbox entries.
fn seed(path: &Path) {
    let db = OptimisticTxDatabase::builder(path).open().unwrap();
    let open = |name| db.keyspace(name, KeyspaceCreateOptions::default).unwrap();
    let (jobs, schedule, work) = (
        open(JOB_KEYSPACE),
        open(SCHEDULE_INDEX_KEYSPACE),
        open(PLUGIN_WORK_KEYSPACE),
    );
    let (records, pending, outbox) = (
        open(FAMILY_RECORD_KEYSPACE),
        open(FAMILY_PENDING_KEYSPACE),
        open(FAMILY_OUTBOX_KEYSPACE),
    );
    let (install_id, work_id) = (Ulid::from_bytes([5u8; 16]), Ulid::from_bytes([6u8; 16]));
    let rows = [
        job(job_id(1), execution(), JobState::Succeeded),
        job(
            cleanup_job_id(job_id(1)),
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: None,
            },
            JobState::Running,
        ),
        job(job_id(2), work_spec(install_id, work_id), JobState::Failed),
        job(job_id(3), execution(), JobState::Cancelled),
    ];
    let mut txn = db.write_tx().unwrap();
    for record in &rows {
        let key = job_record_key(record.job_id);
        txn.insert(&jobs, key.to_vec(), record.to_bytes().unwrap());
    }
    for (time_ms, id) in [(2_000, 1), (3_000, 2), (4_000, 3), (NOW_MS + 1, 4)] {
        txn.insert(&schedule, job_prune_key(time_ms, job_id(id)).to_vec(), []);
    }
    txn.insert(&schedule, due_index_key(5_000, job_id(5)).to_vec(), []);
    let identity = Identity::Call {
        run_id: Ulid::from_bytes([4u8; 16]),
        ordinal: 0,
    };
    let row = row_key(install_id, &identity).unwrap();
    txn.insert(&work, index_key(install_id, work_id), row.clone());
    let row_bytes = postcard::to_allocvec(&work_row(work_id, WorkState::Dispatched)).unwrap();
    txn.insert(&work, row, row_bytes);
    let envelope = claim();
    let key = envelope.key().to_bytes();
    txn.insert(&records, key, postcard::to_allocvec(&envelope).unwrap());
    for (seed, first_seen_ms) in [(1u8, 500), (2u8, 700)] {
        let row = PendingRecord {
            envelope: envelope.clone(),
            need: PendingNeed::LocalView,
            first_seen_ms,
            attempts: 0,
        };
        txn.insert(&pending, [seed], postcard::to_allocvec(&row).unwrap());
    }
    for (seed, queued_at_ms) in [(1u8, NOW_MS - 60_000), (2u8, NOW_MS - 9 * 24 * 3_600_000)] {
        let entry = OutboxEntry {
            queued_at_ms,
            delivered: Vec::new(),
            next_holder: 0,
            rejections: 0,
        };
        txn.insert(&outbox, [seed], postcard::to_allocvec(&entry).unwrap());
    }
    txn.commit().unwrap().unwrap();
}

/// Builds the report at `NOW_MS` and reads back its first section, which holds the job records.
fn job_records(path: &Path) -> JobRecords {
    let report = build_report(path.to_str().unwrap(), NOW_MS).unwrap();
    assert_eq!(report.sections[0].heading, "Job records");
    serde_json::from_value(report.sections[0].content.clone()).unwrap()
}

#[test]
fn reports_known_rows() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("db");
    seed(&path);

    let report = job_records(&path);

    assert_eq!(report.rows.get(JOB_KEYSPACE), Some(&4));
    assert_eq!(report.jobs["execution"]["succeeded"], 1);
    assert_eq!(report.jobs["execution"]["cancelled"], 1);
    assert_eq!(report.jobs["plugin_work"]["failed"], 1);
    assert_eq!(report.jobs["probe"]["running"], 1);
    assert_eq!(report.schedule.prune, 4);
    assert_eq!(report.schedule.due, 1);
    assert_eq!(
        report.schedule.overdue, 3,
        "the future prune row is not overdue"
    );
    assert_eq!(report.schedule.oldest_overdue_ms, Some(2_000));
    assert_eq!(report.held.get("cleanup_pending"), Some(&1));
    assert_eq!(report.held.get("plugin_work"), Some(&1));
    assert_eq!(report.held.values().sum::<u64>(), 2, "job 3 holds nothing");
    assert_eq!(report.families.records.get("claim"), Some(&1));
    assert_eq!(report.families.pending, 2);
    assert_eq!(report.families.oldest_pending_ms, Some(500));
    assert_eq!(report.outbox.entries, 2);
    assert_eq!(
        report.outbox.oldest_queued_ms,
        Some(NOW_MS - 9 * 24 * 3_600_000)
    );
    assert_eq!(report.outbox.ages.get("hour"), Some(&1));
    assert_eq!(report.outbox.ages.get("older"), Some(&1));
    assert_eq!(report.plugins.work.get("dispatched"), Some(&1));
    assert_eq!(report.plugins.not_notified, 1);
}

#[test]
fn creates_no_keyspace() {
    let temp = tempdir().unwrap();
    let path = temp.path().join("db");
    seed(&path);
    let names = || {
        let db = OptimisticTxDatabase::builder(&path).open().unwrap();
        let mut names = db
            .list_keyspace_names()
            .iter()
            .map(|name| name.as_ref().to_string())
            .collect::<Vec<_>>();
        names.sort();
        names
    };
    let before = names();

    let report = job_records(&path);

    assert_eq!(names(), before);
    assert!(!before.iter().any(|name| name == PLUGIN_RUNS_KEYSPACE));
    assert!(report.plugins.runs.is_empty());
    assert!(!report.rows.contains_key(PLUGIN_RUNS_KEYSPACE));
}

#[test]
fn refuses_missing_database() {
    // A wrong path is refused before Fjall could create an empty database there, so no report
    // of zeros is printed and nothing is written.
    let temp = tempdir().unwrap();
    let absent = temp.path().join("absent");
    assert!(build_report(absent.to_str().unwrap(), NOW_MS).is_err());
    assert!(!absent.exists());

    let empty = temp.path().join("empty");
    std::fs::create_dir(&empty).unwrap();
    assert!(build_report(empty.to_str().unwrap(), NOW_MS).is_err());
    assert_eq!(std::fs::read_dir(&empty).unwrap().count(), 0);
}
