use super::super::*;
use super::*;

pub(super) use super::fixtures::*;

#[test]
fn blocked_batch_waits() {
    // Nothing processed means the due head is blocked, so the next scan
    // waits instead of respinning at the 25ms batch pace.
    let blocked = MetadataMaterializationDrainResult {
        processed: 0,
        has_more_due: true,
        next_due_after: None,
    };
    let progressing = MetadataMaterializationDrainResult {
        processed: 4,
        ..blocked
    };
    assert_eq!(drain_delay(&blocked), METADATA_MATERIALIZATION_RETRY_AFTER);
    assert_eq!(
        drain_delay(&progressing),
        METADATA_MATERIALIZATION_NEXT_BATCH_AFTER
    );
}

#[test]
fn reclaim_retry_climbs() {
    // A failing sweep earns the fast retry, then doubles up to the normal
    // interval so a candidate that always fails cannot hot-loop a full
    // rescan every minute.
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let handler = OperationsTaskHandler::new(
        Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }),
        JobsRuntime::new(),
    );
    let key = TaskKey::DrainBlobReclaimQueue;
    let ladder = |handler: &OperationsTaskHandler| {
        handler.retry_ladder(&key, RECLAIM_SWEEP_RETRY, RECLAIM_SWEEP_AFTER)
    };

    assert_eq!(ladder(&handler), RECLAIM_SWEEP_RETRY);
    assert_eq!(ladder(&handler), RECLAIM_SWEEP_RETRY * 2);
    for _ in 0..8 {
        ladder(&handler);
    }
    assert_eq!(ladder(&handler), RECLAIM_SWEEP_AFTER);

    handler.reset_backoff(&key);
    assert_eq!(ladder(&handler), RECLAIM_SWEEP_RETRY);
}

#[tokio::test]
async fn paused_runtime_waits() {
    // Startup recovery must wait until production has made S3 reachable.
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });
    let job_id = job_id();
    let record = JobRecord::new(
        job_id,
        JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        },
        UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
        node(7),
        1,
        1,
        None,
    );
    insert_job(&storage, &record).await.expect("insert job");
    assert!(matches!(
        claim_job(&storage, job_id, node(7), 2).await,
        Ok(ClaimOutcome::Claimed(_))
    ));
    let runtime = JobsRuntime::new_paused();

    initialize_task_incoming(context, task_handle, runtime.clone()).await;

    let stored = read_job_record(&storage, job_id, None)
        .await
        .expect("read job")
        .expect("job exists");
    assert_eq!(stored.state, JobState::Claimed);
    assert_eq!(runtime.recover_stale_jobs(&storage).await.unwrap(), 1);
    runtime.start();
    let stored = read_job_record(&storage, job_id, None)
        .await
        .expect("read job")
        .expect("job exists");
    assert_eq!(stored.state, JobState::Queued);
}

#[tokio::test]
async fn start_keeps_job() {
    let temp_dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
        compute_handle: None,
    });
    let job_id = job_id();
    let record = JobRecord::new(
        job_id,
        JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        },
        UserId::new(Ulid::from_bytes([3u8; 16]), RealmId([2u8; 32])),
        node(7),
        1,
        1,
        None,
    );
    insert_job(&storage, &record).await.expect("insert job");
    assert!(matches!(
        claim_job(&storage, job_id, node(7), 2).await,
        Ok(ClaimOutcome::Claimed(_))
    ));
    let runtime = JobsRuntime::new_paused();

    let queues = initialize_task_holder(
        context,
        task_handle.clone(),
        runtime.clone(),
        RoCrateLimits::default(),
    )
    .await;
    let shutdown = Shutdown::new();
    queues.start(&shutdown).await;

    let stored = read_job_record(&storage, job_id, None)
        .await
        .expect("read job")
        .expect("job exists");
    assert_eq!(stored.state, JobState::Claimed);
    assert!(!runtime.is_started());

    let requested = Duration::from_secs(7200);
    let TaskEvent::TimerScheduled { after, .. } = task_handle
        .schedule_timer_if_idle(TaskKey::DrainJobQueue, requested)
        .await
    else {
        panic!("expected timer schedule event");
    };
    assert_eq!(after, requested, "job queue timer must remain unscheduled");
    assert!(shutdown.drain(Duration::from_secs(30)).await);
}
