//! Tests the jobs of a bucket: start, pause, resume and abort, and pages run by the worker.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0
use super::*;
use crate::driver::drive;
use crate::s3::bucket::rename::tests::{
    BUCKET, access, get, group, head_key, object, realm, setup,
};
use crate::tests::records::node;
use aruna_core::keyspaces::BLOB_HEAD_KEYSPACE;
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
    assert_eq!(
        (ended.finished_at_ms, aborted.aborted_at_ms),
        (Some(5), Some(5))
    );
    assert!(
        get(&context, COMPRESSION_QUEUE_KEYSPACE, bucket)
            .await
            .is_none()
    );
    let after = act(&context, LinkedAction::Resume).await;
    assert!(matches!(after, Err(LinkedError::NoJob(_))));
}
