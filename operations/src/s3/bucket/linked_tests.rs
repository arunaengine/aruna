//! Tests the jobs of a bucket: start, pause, resume and abort, and pages run by the worker.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
use super::*;
use crate::driver::drive;
use crate::s3::bucket::job_pages::{job_access, process_linked, still_admin, store};
use crate::s3::bucket::job_progress::JobStep;
use crate::s3::bucket::rename::tests::{
    BUCKET, access, get, group, head_key, object, realm, setup,
};
use crate::tests::records::node;
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, COMPRESSION_QUEUE_KEYSPACE};
use aruna_core::structs::storage::backend_import::TokenGrant;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::linked_job::RemapPlan;
use ulid::Ulid;

fn grant() -> TokenGrant {
    TokenGrant {
        token_hash: "token".to_string(),
        issuer: realm(),
        issued_at_secs: 1,
        expires_at_secs: u64::MAX,
        via: None,
    }
}

fn job(auth: aruna_core::structs::identity::auth::AuthContext) -> LinkedJob {
    LinkedJob {
        id: Ulid::generate(),
        task: LinkedTask::Remap(RemapPlan {
            from: String::new(),
            pattern: "*.faa.gz".to_string(),
            template: "proteins/{name}".to_string(),
            subfolders: false,
        }),
        group_id: group(),
        auth,
        node_id: node(4),
        grant: grant(),
        state: LinkedState::Running,
        cursor: None,
        total: 2,
        counted: true,
        done: 0,
        skipped: 0,
        failed: 0,
        waiting: None,
        started_at_ms: u64::MAX,
        updated_at_ms: 0,
        finished_at_ms: None,
    }
}

async fn act(context: &DriverContext, action: LinkedAction) -> Result<BucketJob, LinkedError> {
    let operation = BucketJobOperation::new(BUCKET.to_string(), group(), action, 5);
    drive(operation, context).await
}

async fn stored(context: &DriverContext) -> LinkedJob {
    let value = get(context, LINKED_JOB_KEYSPACE, BUCKET.as_bytes().to_vec());
    LinkedJob::from_bytes(&value.await.unwrap()).unwrap()
}

#[tokio::test]
async fn controls_one_job() {
    let (_directory, context) = setup().await;
    let auth = access(&context).await;
    let first = job(auth.clone());
    act(&context, LinkedAction::Start(Box::new(first.clone())))
        .await
        .unwrap();
    // One job holds the bucket until it ends.
    let again = act(&context, LinkedAction::Start(Box::new(job(auth.clone())))).await;
    assert!(matches!(again, Err(LinkedError::Busy(_))));
    act(&context, LinkedAction::Pause).await.unwrap();
    assert_eq!(stored(&context).await.state, LinkedState::Paused);
    let twice = act(&context, LinkedAction::Pause).await;
    assert!(matches!(twice, Err(LinkedError::NoJob(_))));
    // A paused job makes no progress.
    assert_eq!(process_linked(&context).await.unwrap(), None);
    // Abort while paused ends the job for good, with its counts.
    act(&context, LinkedAction::Abort).await.unwrap();
    let ended = stored(&context).await;
    assert_eq!(
        (ended.state, ended.finished_at_ms),
        (LinkedState::Aborted, Some(5))
    );
    assert_eq!(ended.remaining(), 2);
    let resumed = act(&context, LinkedAction::Resume).await;
    assert!(matches!(resumed, Err(LinkedError::NoJob(_))));
    // A new job may start after the abort.
    let second = job(auth);
    act(&context, LinkedAction::Start(Box::new(second.clone())))
        .await
        .unwrap();
    assert_eq!(stored(&context).await.id, second.id);
}

#[tokio::test]
async fn runs_and_resumes() {
    let (_directory, context) = setup().await;
    let auth = access(&context).await;
    let id = Ulid::from_parts(10, 1);
    object(&context, "a.faa.gz", &[id], false).await;
    object(&context, "b.faa.gz", &[id], false).await;
    act(&context, LinkedAction::Start(Box::new(job(auth))))
        .await
        .unwrap();
    act(&context, LinkedAction::Pause).await.unwrap();
    assert!(
        get(&context, BLOB_HEAD_KEYSPACE, head_key("a.faa.gz"))
            .await
            .is_some()
    );
    act(&context, LinkedAction::Resume).await.unwrap();
    process_linked(&context).await.unwrap();
    let done = stored(&context).await;
    assert_eq!(
        (done.state, done.done, done.remaining()),
        (LinkedState::Finished, 2, 0)
    );
    assert!(
        get(&context, BLOB_HEAD_KEYSPACE, head_key("proteins/b.faa.gz"))
            .await
            .is_some()
    );
}

#[tokio::test]
async fn abort_keeps_progress() {
    let (_directory, context) = setup().await;
    let auth = access(&context).await;
    let running = job(auth);
    act(&context, LinkedAction::Start(Box::new(running.clone())))
        .await
        .unwrap();
    // An abort while a page runs keeps the abort and adds what the page finished.
    act(&context, LinkedAction::Abort).await.unwrap();
    let step = JobStep {
        cursor: Some("a.faa.gz".to_string()),
        done: 1,
        ..JobStep::default()
    };
    store(&context, BUCKET, &running, step, None).await.unwrap();
    let ended = stored(&context).await;
    assert_eq!((ended.state, ended.done), (LinkedState::Aborted, 1));
    assert_eq!(ended.cursor.as_deref(), Some("a.faa.gz"));
}

#[tokio::test]
async fn controls_renames() {
    let (_directory, context) = setup().await;
    let auth = access(&context).await;
    let mut record = CompressionMigration::new(Compression::Off, 7);
    record.moving = true;
    let request = RenameRequest {
        started_at_ms: 7,
        auth: auth.clone(),
        node_id: node(4),
        grant: grant(),
        paused: false,
        aborted_at_ms: None,
    };
    let bucket = BUCKET.as_bytes().to_vec();
    let rows = [
        (COMPRESSION_MIGRATION_KEYSPACE, record.to_bytes().unwrap()),
        (COMPRESSION_QUEUE_KEYSPACE, Vec::new()),
        (RENAME_MOVE_KEYSPACE, request.to_bytes().unwrap()),
    ];
    for (space, value) in rows {
        crate::s3::bucket::rename::tests::put(&context, space, bucket.clone(), value).await;
    }
    // A running rename holds the bucket against a new job.
    let start = act(&context, LinkedAction::Start(Box::new(job(auth)))).await;
    assert!(matches!(start, Err(LinkedError::Busy(_))));
    let Ok(BucketJob::Rename(_, paused)) = act(&context, LinkedAction::Pause).await else {
        panic!("expected the paused rename")
    };
    assert!(paused.paused);
    let Ok(BucketJob::Rename(ended, aborted)) = act(&context, LinkedAction::Abort).await else {
        panic!("expected the aborted rename")
    };
    // The worker ends the move once its version in flight is done; until then it holds.
    assert_eq!(
        (ended.finished_at_ms, aborted.aborted_at_ms),
        (None, Some(5))
    );
    assert!(
        get(&context, COMPRESSION_QUEUE_KEYSPACE, bucket)
            .await
            .is_some()
    );
    let after = act(&context, LinkedAction::Resume).await;
    assert!(matches!(after, Err(LinkedError::NoJob(_))));
}

#[tokio::test]
async fn needs_group_admin() {
    // A job runs only while its caller still administers the group.
    let (_directory, context) = setup().await;
    let auth = access(&context).await;
    assert!(still_admin(&context, &auth, group()).await.unwrap());
    let stranger = aruna_core::structs::identity::auth::AuthContext {
        user_id: aruna_core::UserId::local(Ulid::from_parts(77, 7), realm()),
        ..auth.clone()
    };
    assert!(!still_admin(&context, &stranger, group()).await.unwrap());
    let mut job = job(stranger);
    job.task = LinkedTask::Unify {
        prefix: String::new(),
    };
    assert!(job_access(&context, BUCKET, &job).await.unwrap().is_none());
}

fn bucket_key() -> Key {
    BUCKET.as_bytes().to_vec().into()
}

fn job_rows(
    linked: Option<&LinkedJob>,
    rename: Option<(&CompressionMigration, &RenameRequest)>,
) -> Event {
    let migration = rename.map(|(record, _)| record.to_bytes().unwrap().into());
    let request = rename.map(|(_, request)| request.to_bytes().unwrap().into());
    Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (
                bucket_key(),
                linked.map(|row| row.to_bytes().unwrap().into()),
            ),
            (bucket_key(), migration),
            (bucket_key(), request),
        ],
    })
}

fn linked_job(state: LinkedState, started_at_ms: u64) -> LinkedJob {
    let mut row = job(aruna_core::structs::identity::auth::AuthContext::anonymous(
        realm(),
    ));
    (row.state, row.started_at_ms) = (state, started_at_ms);
    row
}

fn rename_move(started_at_ms: u64, request_ms: u64) -> (CompressionMigration, RenameRequest) {
    let mut record = CompressionMigration::new(Compression::Off, started_at_ms);
    record.moving = true;
    let request = RenameRequest {
        started_at_ms: request_ms,
        auth: aruna_core::structs::identity::auth::AuthContext::anonymous(realm()),
        node_id: node(4),
        grant: grant(),
        paused: false,
        aborted_at_ms: None,
    };
    (record, request)
}

fn shown(event: Event) -> Result<Option<BucketJob>, LinkedError> {
    let mut operation = ShownJobOperation::new(BUCKET);
    operation.start();
    operation.step(event);
    operation.finalize()
}

#[test]
fn reads_bucket_job() {
    let reads = [
        LINKED_JOB_KEYSPACE,
        COMPRESSION_MIGRATION_KEYSPACE,
        RENAME_MOVE_KEYSPACE,
    ];
    let reads = reads
        .map(|space| (space.to_string(), bucket_key()))
        .to_vec();
    let start = Effect::Storage(StorageEffect::BatchRead {
        reads,
        txn_id: None,
    });
    let running = linked_job(LinkedState::Running, 3);
    let mut operation = ShownJobOperation::new(BUCKET);
    let steps = vec![(job_rows(Some(&running), None), vec![])];
    aruna_core::operation_check::check_trace(&mut operation, &[start], steps);
    assert_eq!(operation.finalize(), Ok(Some(BucketJob::Linked(running))));
}

#[test]
fn shows_job_chosen() {
    // A running job wins; between ended ones the later start wins; another move's rows are not
    // a rename.
    let running = linked_job(LinkedState::Running, 3);
    let early = linked_job(LinkedState::Finished, 5);
    let late = linked_job(LinkedState::Finished, 9);
    let (record, request) = rename_move(7, 7);
    let mut ended = record.clone();
    ended.finished_at_ms = Some(8);
    let (other, unrelated) = rename_move(7, 6);
    let linked = |row: &LinkedJob| Ok(Some(BucketJob::Linked(row.clone())));
    let renamed = |record: &CompressionMigration| {
        Ok(Some(BucketJob::Rename(record.clone(), request.clone())))
    };
    let cases = [
        (Some(&running), Some((&record, &request)), linked(&running)),
        (Some(&early), Some((&record, &request)), renamed(&record)),
        (Some(&late), Some((&ended, &request)), linked(&late)),
        (Some(&early), Some((&ended, &request)), renamed(&ended)),
        (Some(&early), Some((&other, &unrelated)), linked(&early)),
        (None, Some((&ended, &request)), renamed(&ended)),
        (None, None, Ok(None)),
    ];
    for (row, rename, expected) in cases {
        assert_eq!(shown(job_rows(row, rename)), expected);
    }
}

#[test]
fn bucket_job_errors() {
    let failed = Event::Storage(StorageEvent::Error {
        error: StorageError::Timeout,
    });
    assert_eq!(
        shown(failed),
        Err(LinkedError::Storage(StorageError::Timeout))
    );
    let corrupt = Event::Storage(StorageEvent::BatchReadResult {
        values: vec![(bucket_key(), Some(vec![0xff].into()))],
    });
    assert!(matches!(shown(corrupt), Err(LinkedError::Conversion(_))));
}

#[test]
fn reads_rename_total() {
    let row = |started_at_ms| RenameTotal {
        started_at_ms,
        total: 3,
    };
    let read = |value: Option<RenameTotal>| {
        Event::Storage(StorageEvent::ReadResult {
            key: bucket_key(),
            value: value.map(|row| row.to_bytes().unwrap().into()),
        })
    };
    let start = Effect::Storage(StorageEffect::Read {
        key_space: RENAME_TOTAL_KEYSPACE.to_string(),
        key: bucket_key(),
        txn_id: None,
    });
    let mut operation = RenameTotalOperation::new(BUCKET, 7);
    let steps = vec![(read(Some(row(7))), vec![])];
    aruna_core::operation_check::check_trace(&mut operation, &[start], steps);
    assert_eq!(operation.finalize(), Ok(Some(3)));
    // A total stored for another move, or none, is no total.
    for value in [Some(row(8)), None] {
        let mut operation = RenameTotalOperation::new(BUCKET, 7);
        operation.start();
        operation.step(read(value));
        assert_eq!(operation.finalize(), Ok(None));
    }
}

#[test]
fn job_reads_contract() {
    use aruna_core::operation_check::{
        check_abort, rejects_events, rejects_finalize, sample_events,
    };
    let make = || {
        let mut operation = ShownJobOperation::new(BUCKET);
        operation.start();
        operation
    };
    let accepted =
        |event: &Event| matches!(event, Event::Storage(StorageEvent::BatchReadResult { .. }));
    let refused = rejects_events(make, sample_events, accepted);
    assert_eq!(refused.len(), sample_events().len() - 1);
    let make = || {
        let mut operation = RenameTotalOperation::new(BUCKET, 7);
        operation.start();
        operation
    };
    let accepted = |event: &Event| matches!(event, Event::Storage(StorageEvent::ReadResult { .. }));
    let refused = rejects_events(make, sample_events, accepted);
    assert_eq!(refused.len(), sample_events().len() - 1);
    let mut fresh = ShownJobOperation::new(BUCKET);
    check_abort(&mut fresh, &[]);
    rejects_finalize(fresh);
    let mut fresh = RenameTotalOperation::new(BUCKET, 7);
    check_abort(&mut fresh, &[]);
    rejects_finalize(fresh);
}
