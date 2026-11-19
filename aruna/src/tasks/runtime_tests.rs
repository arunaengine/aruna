//! Tests the job runtime registry together with the job runner that spawns, cancels and recovers jobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::tasks::runner::{handle_panic, run_job};
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::JOB_STATE_KEYSPACE;
use aruna_core::structs::execution::job::{
    AttemptIntent, ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec,
    ImportRoCrateTarget, JobClaim, JobPayload, JobResultPayload, RoCrateLimits,
};
use aruna_core::structs::execution::job::{JobError, JobErrorKind, JobState};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::jobs::JOB_MAX_ATTEMPTS;
use aruna_operations::jobs::context::JobContext;
use aruna_operations::jobs::executor::JobRunOutcome;
use aruna_operations::jobs::runtime::*;
use aruna_operations::jobs::runtime_steps::requeue_call;
use aruna_operations::jobs::store::JobMutation;
use aruna_operations::jobs::store::job_calls::{AdoptOutcome, ClaimOutcome, RequeueOutcome};
use aruna_operations::jobs::store::mutation_error::JobMutationError;
use aruna_operations::jobs::store::test_calls::{
    adopt_external_attempt, cancel_running_job, claim_job, complete_job, insert_job,
    list_job_entries, mutate_job, read_job_record, record_attempt_intent, requeue_job,
    set_cancel_requested,
};
use aruna_storage::{FjallStorage, StorageHandle};
use aruna_operations::testing::task_context;
use byteview::ByteView;
use futures_util::future::FutureExt;
use std::time::Duration;
use tempfile::tempdir;
use ulid::Ulid;

fn node_id(seed: u8) -> NodeId {
    let mut seed_bytes = [0u8; 32];
    seed_bytes[0] = seed;
    iroh::SecretKey::from_bytes(&seed_bytes).public()
}

fn probe_record(job_id: JobId, steps: u32, sleep_ms: u64, marker: Option<String>) -> JobRecord {
    JobRecord::new(
        job_id,
        JobPayload::Probe {
            steps,
            step_sleep_ms: sleep_ms,
            fail_at: None,
            panic_at: None,
            cleanup_marker: marker,
        },
        UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
        node_id(7),
        1,
        1,
        None,
    )
}

fn import_record(job_id: JobId) -> JobRecord {
    let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
    JobRecord::new(
        job_id,
        JobPayload::ImportRoCrate(ImportRoCrateSpec {
            auth_context: AuthContext {
                user_id: owner,
                realm_id: RealmId([1u8; 32]),
                path_restrictions: None,
                session: None,
            },
            source: ImportRoCrateSource::Upload {
                upload_id: Ulid::from_bytes([3u8; 16]),
            },
            target: ImportRoCrateTarget {
                bucket: "target".to_string(),
                prefix: "crate".to_string(),
            },
            metadata: ImportMetadataTarget {
                group_id: Ulid::from_bytes([4u8; 16]),
                path: "crate".to_string(),
                public: false,
            },
            limits: RoCrateLimits::default(),
            document_id: Ulid::from_bytes([5u8; 16]),
        }),
        owner,
        node_id(7),
        1,
        1,
        None,
    )
}

async fn claim(context: &DriverContext, record: JobRecord) -> JobRecord {
    insert_job(context, &record).await.unwrap();
    claim_job(context, record.job_id, node_id(3), 1)
        .await
        .unwrap();
    read_job_record(context, record.job_id, None)
        .await
        .unwrap()
        .unwrap()
}

async fn reclaim(context: &DriverContext, job_id: JobId) -> JobRecord {
    claim_job(context, job_id, node_id(3), unix_timestamp_millis())
        .await
        .unwrap();
    read_job_record(context, job_id, None)
        .await
        .unwrap()
        .unwrap()
}

async fn wait_running(context: &DriverContext, job_id: JobId) {
    for _ in 0..400 {
        if let Ok(Some(record)) = read_job_record(context, job_id, None).await
            && record.state == JobState::Running
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("job never reached Running");
}

fn temp_storage() -> (tempfile::TempDir, StorageHandle) {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    (dir, storage)
}

// Admission closes on its own, before the drain spends any budget.
#[test]
fn close_stops_claims() {
    let runtime = JobsRuntime::new();
    assert!(runtime.available_slots() > 0);

    runtime.close_admission();

    assert_eq!(runtime.available_slots(), 0);
    assert!(runtime.shutdown.is_cancelled());
}

#[tokio::test]
async fn probe_reaches_success() {
    let (_dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let runtime = JobsRuntime::new();
    let job_id = JobId::from_bytes([1u8; 16]);
    let claimed = claim(&ctx, probe_record(job_id, 3, 0, None)).await;

    runtime.spawn(ctx.clone(), claimed);
    let state = runtime
        .wait_for_terminal(&ctx, job_id, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(state, JobState::Succeeded);

    let record = read_job_record(&ctx, job_id, None).await.unwrap().unwrap();
    assert_eq!(
        record.result,
        Some(JobResultPayload::Probe { completed_steps: 3 })
    );
}

#[tokio::test]
async fn cancel_running_cleanup() {
    let (dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let runtime = JobsRuntime::new();
    let job_id = JobId::from_bytes([2u8; 16]);
    let marker = dir.path().join("probe-marker");
    let marker_str = marker.to_str().unwrap().to_string();
    let claimed = claim(&ctx, probe_record(job_id, 10_000, 5, Some(marker_str))).await;

    runtime.spawn(ctx.clone(), claimed);
    for _ in 0..100 {
        if marker.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        marker.exists(),
        "probe should create its marker while running"
    );
    assert!(runtime.request_cancel(job_id));

    let state = runtime
        .wait_for_terminal(&ctx, job_id, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(state, JobState::Cancelled);
    // Cleanup follows the stored terminal state, so it may still be running here.
    for _ in 0..1000 {
        if !marker.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!marker.exists(), "cleanup hook must remove the marker");
}

#[tokio::test]
async fn cancel_before_run() {
    let (dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let runtime = JobsRuntime::new();
    let job_id = JobId::from_bytes([3u8; 16]);
    let marker = dir.path().join("marker");
    std::fs::write(&marker, b"partial").unwrap();
    let mut record = probe_record(job_id, 5, 0, Some(marker.to_str().unwrap().to_string()));
    record.cancel_requested = true;
    // A prior run means it is claimed (not fresh-cancelled) so cleanup runs.
    record.has_run = true;
    let claimed = claim(&ctx, record).await;

    runtime.spawn(ctx.clone(), claimed);
    let state = runtime
        .wait_for_terminal(&ctx, job_id, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(state, JobState::Cancelled);
    assert!(!marker.exists(), "cleanup removes partial state on reclaim");
}

#[tokio::test]
async fn cancel_runs_import() {
    let (_dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let job_id = JobId::from_bytes([0x3B; 16]);
    let mut record = import_record(job_id);
    record.cancel_requested = true;
    record.has_run = true;
    let claimed = claim(&ctx, record).await;
    let token = claimed.claim.as_ref().unwrap().claim_token;

    run_job(
        ctx.clone(),
        claimed,
        token,
        (CancellationToken::new(), None),
        (CancellationToken::new(), RangeTurns::default(), Vec::new()),
    )
    .await;

    let stored = read_job_record(&ctx, job_id, None).await.unwrap().unwrap();
    assert_eq!(stored.state, JobState::Cancelled);
    assert!(matches!(
        storage
            .send_storage_effect(StorageEffect::Read {
                key_space: JOB_STATE_KEYSPACE.to_string(),
                key: ByteView::from(job_id.to_bytes().to_vec()),
                txn_id: None,
            })
            .await,
        Event::Storage(aruna_core::events::StorageEvent::ReadResult { value: Some(_), .. })
    ));
}

// A cancel committed after claim must beat payload execution from the stale snapshot.
#[tokio::test]
async fn cancel_after_claim() {
    let (dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let job_id = JobId::from_bytes([0x3A; 16]);
    let marker = dir.path().join("late-cancel-marker");
    std::fs::write(&marker, b"partial").unwrap();
    let mut record = probe_record(job_id, 1, 0, Some(marker.to_str().unwrap().to_string()));
    let JobPayload::Probe { fail_at, .. } = &mut record.payload else {
        panic!("probe payload");
    };
    *fail_at = Some(0);
    let claimed = claim(&ctx, record).await;
    let token = claimed.claim.as_ref().unwrap().claim_token;
    set_cancel_requested(&ctx, job_id, unix_timestamp_millis())
        .await
        .unwrap();

    run_job(
        ctx.clone(),
        claimed,
        token,
        (CancellationToken::new(), None),
        (CancellationToken::new(), RangeTurns::default(), Vec::new()),
    )
    .await;

    let stored = read_job_record(&ctx, job_id, None).await.unwrap().unwrap();
    assert_eq!(stored.state, JobState::Cancelled);
    assert!(!marker.exists(), "late cancellation must run cleanup");
}

// Cleanup is claim-owned; a reclaimed zombie must leave the side effect alone.
#[tokio::test]
async fn zombie_skips_cleanup() {
    let (dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let job_id = JobId::from_bytes([0x2A; 16]);
    let marker = dir.path().join("zombie-marker");
    let marker_str = marker.to_str().unwrap().to_string();
    let claimed = claim(&ctx, probe_record(job_id, 1, 60_000, Some(marker_str))).await;
    let stale_token = claimed.claim.as_ref().unwrap().claim_token;
    let cancel = CancellationToken::new();
    let execution = tokio::spawn(run_job(
        ctx.clone(),
        claimed,
        stale_token,
        (cancel.clone(), None),
        (CancellationToken::new(), RangeTurns::default(), Vec::new()),
    ));
    for _ in 0..100 {
        if marker.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(marker.exists(), "probe must reach its cleanup side effect");

    requeue_job(
        &ctx,
        job_id,
        Some(stale_token),
        unix_timestamp_millis(),
        None,
        None,
    )
    .await
    .unwrap();
    assert!(matches!(
        claim_job(&ctx, job_id, node_id(4), unix_timestamp_millis())
            .await
            .unwrap(),
        ClaimOutcome::Claimed(_)
    ));
    assert!(
        !execution.is_finished(),
        "stale payload must still be running"
    );

    cancel.cancel();
    execution.await.unwrap();
    assert!(marker.exists(), "zombie must not run cleanup");
}

#[tokio::test]
async fn completion_wins_race() {
    let (_dir, storage) = temp_storage();
    let context = aruna_operations::testing::test_context(storage.clone());
    let job_id = JobId::from_bytes([4u8; 16]);
    let mut record = probe_record(job_id, 1, 0, None);
    record.state = JobState::Running;
    let token = Ulid::generate();
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: token,
        lease_expires_ms: 60_000,
    });
    insert_job(&context, &record).await.unwrap();

    complete_job(
        &context,
        job_id,
        token,
        JobResultPayload::Probe { completed_steps: 1 },
        record.progress.clone(),
        5_000,
    )
    .await
    .unwrap();

    // The cancel loser hits a cleared claim and is rejected without corrupting state.
    let loser = cancel_running_job(&context, job_id, token, 5_001).await;
    assert!(matches!(loser, Err(JobMutationError::TokenMismatch)));
    let record = read_job_record(&context, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, JobState::Succeeded);
}

#[tokio::test]
async fn recover_requeues_inflight() {
    let (_dir, storage) = temp_storage();
    let context = aruna_operations::testing::test_context(storage.clone());
    let runtime = JobsRuntime::new();
    let job_id = JobId::from_bytes([5u8; 16]);
    let mut record = probe_record(job_id, 3, 0, None);
    record.state = JobState::Running;
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: Ulid::generate(),
        lease_expires_ms: unix_timestamp_millis() + 60_000,
    });
    insert_job(&context, &record).await.unwrap();

    for restart in 0..(JOB_MAX_ATTEMPTS + 2) {
        assert_eq!(runtime.recover_stale_jobs(&context).await.unwrap(), 1);
        let recovered = read_job_record(&context, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.state, JobState::Queued, "restart {restart}");
        assert_eq!(recovered.attempts, 0, "restart {restart}");
        assert!(recovered.claim.is_none());
        if restart < JOB_MAX_ATTEMPTS + 1 {
            reclaim(&context, job_id).await;
        }
    }
}

// finished_at must reflect completion time, not the job's start.
#[tokio::test]
async fn finished_at_fresh() {
    let (_dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let runtime = JobsRuntime::new();
    let job_id = JobId::from_bytes([8u8; 16]);
    let claimed = claim(&ctx, probe_record(job_id, 30, 10, None)).await;

    let t_before = unix_timestamp_millis();
    runtime.spawn(ctx.clone(), claimed);
    let state = runtime
        .wait_for_terminal(&ctx, job_id, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(state, JobState::Succeeded);

    let record = read_job_record(&ctx, job_id, None).await.unwrap().unwrap();
    assert!(
        record.finished_at_ms.unwrap() >= t_before + 150,
        "finished_at must be set after the payload ran, not at start"
    );
}

// Perf budget: progress is throttled, so a 10k-step job writes O(1), not O(steps).
#[tokio::test]
async fn progress_writes_throttled() {
    let (_dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let runtime = JobsRuntime::new();
    let job_id = JobId::from_bytes([7u8; 16]);
    let claimed = claim(&ctx, probe_record(job_id, 10_000, 0, None)).await;

    let before = storage.snapshot_metrics().requests_total;
    runtime.spawn(ctx.clone(), claimed);
    let state = runtime
        .wait_for_terminal(&ctx, job_id, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(state, JobState::Succeeded);

    let delta = storage.snapshot_metrics().requests_total - before;
    assert!(
        delta < 200,
        "10k-step run must not scale with steps, got {delta}"
    );

    let record = read_job_record(&ctx, job_id, None).await.unwrap().unwrap();
    assert_eq!(record.progress.current, 10_000);
}

// A job interrupted by a shutdown is queued again, and the restart costs it nothing.
#[tokio::test]
async fn shutdown_releases_lease() {
    let (_dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let job_id = JobId::from_bytes([0x11; 16]);
    let claimed = claim(&ctx, probe_record(job_id, 100, 20, None)).await;

    let runtime = JobsRuntime::new();
    runtime.spawn(ctx.clone(), claimed);
    wait_running(&ctx, job_id).await;
    let report = runtime.shutdown(&ctx, Duration::ZERO).await;
    // The payload's own Interrupted release races the shutdown sweep; both
    // hand the lease back attempt-free, so only the sum is deterministic.
    assert_eq!(report.released + report.finished + report.skipped, 1);

    let record = read_job_record(&ctx, job_id, None).await.unwrap().unwrap();
    assert_eq!(record.state, JobState::Queued);
    assert_eq!(record.attempts, 0, "a shutdown is not a spent attempt");
    assert!(record.claim.is_none());
    assert!(
        record.due_at_ms <= unix_timestamp_millis(),
        "due immediately"
    );

    // The next node picks it straight back up and drives it to success.
    let restarted = JobsRuntime::new();
    let reclaimed = reclaim(&ctx, job_id).await;
    restarted.spawn(ctx.clone(), reclaimed);
    let state = restarted
        .wait_for_terminal(&ctx, job_id, Duration::from_secs(20))
        .await
        .unwrap();
    assert_eq!(state, JobState::Succeeded);
    let record = read_job_record(&ctx, job_id, None).await.unwrap().unwrap();
    assert_eq!(record.attempts, 0);
}

#[tokio::test]
async fn shutdown_awaits_producer() {
    let (_dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let job_id = JobId::from_bytes([0x15; 16]);
    let claimed = claim(&ctx, probe_record(job_id, 1, 0, None)).await;
    let runtime = JobsRuntime::new();
    let producer = runtime.claim_producer().await.unwrap();

    let mut shutdown = Box::pin(runtime.shutdown(&ctx, Duration::ZERO));
    assert!(
        shutdown.as_mut().now_or_never().is_none(),
        "shutdown must wait for the active claim producer"
    );

    runtime.spawn(ctx.clone(), claimed);
    drop(producer);
    tokio::time::timeout(Duration::from_secs(30), shutdown)
        .await
        .expect("shutdown waits only for the producer barrier");

    let record = read_job_record(&ctx, job_id, None).await.unwrap().unwrap();
    assert_eq!(record.state, JobState::Queued);
    assert!(record.claim.is_none());
    assert_eq!(record.attempts, 0);
}

// A committed checkpoint asks the heartbeat to persist its progress without waiting.
#[test]
fn checkpoint_requests_flush() {
    let record = probe_record(JobId::from_bytes([0x17; 16]), 1, 0, None);
    let progress = ProgressReporter::from_progress(&record.progress);
    assert!(progress.flush_requested().now_or_never().is_none());
    progress.checkpoint();
    assert!(progress.flush_requested().now_or_never().is_some());
    assert!(progress.checkpointed());
}

// Transient failures after a committed checkpoint never exhaust a long job; failures
// without progress still do.
#[tokio::test]
async fn progress_spares_attempts() {
    let (_dir, storage) = temp_storage();
    let context = aruna_operations::testing::test_context(storage.clone());
    let job_id = JobId::from_bytes([0x16; 16]);
    insert_job(&context, &probe_record(job_id, 1, 0, None))
        .await
        .unwrap();
    let failure = JobError::retryable("source timed out");

    for failure_no in 0..(JOB_MAX_ATTEMPTS + 2) {
        let claimed = reclaim(&context, job_id).await;
        let token = claimed.claim.unwrap().claim_token;
        let progress = ProgressReporter::from_progress(&claimed.progress);
        progress.checkpoint();
        let outcome = drive(
            requeue_call(
                job_id,
                token,
                unix_timestamp_millis(),
                failure.clone(),
                progress.checkpointed(),
            ),
            &context,
        )
        .await
        .unwrap();
        assert!(
            matches!(outcome, RequeueOutcome::Requeued(_)),
            "failure {failure_no}"
        );
    }

    for _ in 2..JOB_MAX_ATTEMPTS {
        let claimed = reclaim(&context, job_id).await;
        let token = claimed.claim.unwrap().claim_token;
        let progress = ProgressReporter::from_progress(&claimed.progress);
        let outcome = drive(
            requeue_call(
                job_id,
                token,
                unix_timestamp_millis(),
                failure.clone(),
                progress.checkpointed(),
            ),
            &context,
        )
        .await
        .unwrap();
        assert!(matches!(outcome, RequeueOutcome::Requeued(_)));
    }
    let claimed = reclaim(&context, job_id).await;
    let token = claimed.claim.unwrap().claim_token;
    let progress = ProgressReporter::from_progress(&claimed.progress);
    let outcome = drive(
        requeue_call(
            job_id,
            token,
            unix_timestamp_millis(),
            failure,
            progress.checkpointed(),
        ),
        &context,
    )
    .await
    .unwrap();
    assert!(matches!(outcome, RequeueOutcome::Exhausted(_)));
}

// Rolling restarts must never exhaust JOB_MAX_ATTEMPTS on a job that never failed.
#[tokio::test]
async fn restarts_keep_attempts() {
    let (_dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let job_id = JobId::from_bytes([0x12; 16]);
    insert_job(&ctx, &probe_record(job_id, 10_000, 5, None))
        .await
        .unwrap();

    for restart in 0..(JOB_MAX_ATTEMPTS + 2) {
        let runtime = JobsRuntime::new();
        let claimed = reclaim(&ctx, job_id).await;
        runtime.spawn(ctx.clone(), claimed);
        wait_running(&ctx, job_id).await;
        runtime.shutdown(&ctx, Duration::ZERO).await;

        let record = read_job_record(&ctx, job_id, None).await.unwrap().unwrap();
        assert_eq!(record.state, JobState::Queued, "restart {restart}");
        assert_eq!(record.attempts, 0, "restart {restart}");
    }
}

// Shutdown is not cancellation: no Cancelled state and no cleanup handler.
#[tokio::test]
async fn shutdown_skips_cleanup() {
    let (dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let job_id = JobId::from_bytes([0x13; 16]);
    let marker = dir.path().join("probe-marker");
    let marker_str = marker.to_str().unwrap().to_string();
    let claimed = claim(&ctx, probe_record(job_id, 10_000, 5, Some(marker_str))).await;

    let runtime = JobsRuntime::new();
    runtime.spawn(ctx.clone(), claimed);
    wait_running(&ctx, job_id).await;
    for _ in 0..100 {
        if marker.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(marker.exists(), "probe should create its marker");

    let report = runtime.shutdown(&ctx, Duration::from_secs(10)).await;
    assert_eq!(report.finished, 1, "the probe winds down inside the grace");

    let record = read_job_record(&ctx, job_id, None).await.unwrap().unwrap();
    assert_eq!(record.state, JobState::Queued);
    assert_eq!(record.attempts, 0);
    assert!(marker.exists(), "cleanup must not run for an interruption");
}

// Releasing a lease another node holds would double-run the job.
#[tokio::test]
async fn shutdown_spares_takeover() {
    let (_dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let job_id = JobId::from_bytes([0x14; 16]);
    let claimed = claim(&ctx, probe_record(job_id, 10_000, 5, None)).await;

    let runtime = JobsRuntime::new();
    runtime.spawn(ctx.clone(), claimed);
    wait_running(&ctx, job_id).await;

    // The lease lapses and a peer takes the job over while we still hold it locally.
    requeue_job(&ctx, job_id, None, unix_timestamp_millis(), None, None)
        .await
        .unwrap();
    claim_job(&ctx, job_id, node_id(4), unix_timestamp_millis())
        .await
        .unwrap();

    let report = runtime.shutdown(&ctx, Duration::ZERO).await;
    assert_eq!(report.released, 0);
    assert_eq!(report.skipped, 1);

    let record = read_job_record(&ctx, job_id, None).await.unwrap().unwrap();
    assert_eq!(record.state, JobState::Claimed);
    assert_eq!(record.claim.unwrap().holder_node_id, node_id(4));
}

// A saturated executor never drains, so a never-run job must terminalize in the store
// transaction itself rather than wait for a claim that is not coming.
#[tokio::test]
async fn cancel_beats_capacity() {
    let (_dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let runtime = JobsRuntime::with_capacity(0);
    let job_id = JobId::from_bytes([0x6C; 16]);
    let record = probe_record(job_id, 5, 0, None);
    let owner = record.created_by;
    insert_job(&ctx, &record).await.unwrap();
    assert_eq!(runtime.available_slots(), 0);

    let outcome = cancel_owned_job(&ctx, &runtime, owner, job_id)
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        aruna_operations::jobs::job_cancel::CancelJobOutcome::Requested(_)
    ));

    let stored = read_job_record(&ctx, job_id, None).await.unwrap().unwrap();
    assert_eq!(stored.state, JobState::Cancelled);
    assert!(stored.finished_at_ms.is_some());
    assert!(stored.claim.is_none());
}

#[tokio::test]
async fn wait_times_out() {
    let (_dir, storage) = temp_storage();
    let context = aruna_operations::testing::test_context(storage.clone());
    let runtime = JobsRuntime::new();
    let job_id = JobId::from_bytes([6u8; 16]);
    insert_job(&context, &probe_record(job_id, 1, 0, None))
        .await
        .unwrap();

    let result = runtime
        .wait_for_terminal(&context, job_id, Duration::from_millis(100))
        .await;
    assert_eq!(result, None);
}

// A panicking payload must free its slot, not wedge the runtime forever.
#[tokio::test]
async fn panic_frees_slot() {
    let (_dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let runtime = JobsRuntime::with_capacity(1);
    let boom = JobId::from_bytes([0xEE; 16]);
    let mut record = probe_record(boom, 5, 0, None);
    let JobPayload::Probe { panic_at, .. } = &mut record.payload else {
        unreachable!()
    };
    *panic_at = Some(0);
    let claimed = claim(&ctx, record).await;

    runtime.spawn(ctx.clone(), claimed);
    let mut freed = false;
    for _ in 0..400 {
        if runtime.available_slots() == 1 {
            freed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(freed, "panic must release the concurrency slot");

    let ok = JobId::from_bytes([0xAB; 16]);
    let claimed = claim(&ctx, probe_record(ok, 2, 0, None)).await;
    runtime.spawn(ctx.clone(), claimed);
    let state = runtime
        .wait_for_terminal(&ctx, ok, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(
        state,
        JobState::Succeeded,
        "runtime still drains after a panic"
    );
}

// A zombie execution's finish must not evict the newer execution that replaced it.
#[tokio::test]
async fn zombie_keeps_survivor() {
    let (_dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let runtime = JobsRuntime::new();
    let job_id = JobId::from_bytes([0x5A; 16]);

    let zombie = runtime.register_test_execution(job_id, JobExecutionClass::InProcess);
    let survivor = runtime.register_test_execution(job_id, JobExecutionClass::InProcess);
    assert_ne!(zombie, survivor);

    runtime.finish(&ctx, job_id, zombie).await;
    assert_eq!(runtime.running_count(), 1, "zombie must not evict survivor");
    assert!(runtime.request_cancel(job_id), "survivor still registered");

    runtime.finish(&ctx, job_id, survivor).await;
    assert_eq!(runtime.running_count(), 0);
}

// A delivery that cannot read a quorum waits for its delay without spending an attempt.
#[tokio::test]
async fn deferred_for_delay() {
    use aruna_core::structs::execution::notification::{
        NotificationClass, NotificationKind, NotificationRecord,
    };
    use aruna_core::structs::execution::webhook::WebhookDeliverySpec;
    let (_dir, storage) = temp_storage();
    let driver = task_context(storage.clone());
    let runtime = JobsRuntime::new();
    let job_id = JobId::from_bytes([0x6D; 16]);
    let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
    let kind = NotificationKind::ShareUpload {
        share_id: Ulid::from_bytes([3u8; 16]),
        node_id: node_id(7),
        group_id: Ulid::from_bytes([4u8; 16]),
        bucket: "inbox".to_string(),
        key: "a.csv".to_string(),
        size_bytes: 1,
    };
    let now = unix_timestamp_millis();
    let notification = NotificationRecord::new(owner, NotificationClass::Direct, kind, now);
    let payload = JobPayload::WebhookDelivery(WebhookDeliverySpec {
        subscription_id: Ulid::nil(),
        notification,
    });
    let record = JobRecord::new(job_id, payload, owner, node_id(7), 1, 1, None);
    let claimed = claim(&driver, record).await;
    let attempts = claimed.attempts;

    runtime.spawn(task_context(storage.clone()), claimed);
    let mut finished = runtime.state().running[&job_id].completion.subscribe();
    tokio::time::timeout(Duration::from_secs(60), finished.wait_for(|done| *done))
        .await
        .expect("delivery finishes its attempt")
        .unwrap();
    let stored = read_job_record(&driver, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (stored.state, stored.attempts),
        (JobState::Queued, attempts)
    );
    assert!(stored.claim.is_none());
    assert!(
        stored.due_at_ms >= now + 30_000,
        "the job waits for its delay"
    );
    assert!(stored.last_error.is_some());
}

// The node telemetry counts a job from its spawn until its finish, imports apart.
#[tokio::test]
async fn counts_running_jobs() {
    let (_dir, storage) = temp_storage();
    let ctx = task_context(storage.clone());
    let runtime = JobsRuntime::new();
    let job_id = JobId::from_bytes([0x6B; 16]);
    let claimed = claim(&ctx, probe_record(job_id, 2, 0, None)).await;

    runtime.spawn(ctx, claimed);
    assert_eq!(
        runtime.running_jobs(),
        RunningJobs {
            total: 1,
            imports: 0
        }
    );
    let mut finished = runtime.state().running[&job_id].completion.subscribe();
    tokio::time::timeout(Duration::from_secs(60), finished.wait_for(|done| *done))
        .await
        .expect("probe finishes")
        .unwrap();
    assert_eq!(runtime.running_jobs(), RunningJobs::default());

    let nonce = runtime.register_test_execution(job_id, JobExecutionClass::InProcess);
    for kind in [
        JobKind::Staging,
        JobKind::ImportRoCrate,
        JobKind::BackendImport,
    ] {
        runtime.state().running.get_mut(&job_id).unwrap().view.kind = kind;
        assert_eq!(
            runtime.running_jobs(),
            RunningJobs {
                total: 1,
                imports: 1
            }
        );
    }
    runtime.finish(&task_context(storage), job_id, nonce).await;
    assert_eq!(runtime.running_jobs(), RunningJobs::default());
}

// The list carries what an import works on, and progress only once the job counts.
#[tokio::test]
async fn lists_running_jobs() {
    let runtime = JobsRuntime::new();
    let job_id = JobId::from_bytes([0x6C; 16]);
    runtime.register_test_execution(job_id, JobExecutionClass::InProcess);
    let reporter = ProgressReporter::from_progress(
        &aruna_core::structs::execution::job::JobProgress::new("entries"),
    );
    {
        let mut state = runtime.state();
        let job = state.running.get_mut(&job_id).unwrap();
        job.view = RunningJobView::new(&import_record(job_id), 42);
        job.progress = Some(reporter.clone());
    }
    let listed = runtime.running_list();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].kind, JobKind::ImportRoCrate);
    assert_eq!(
        listed[0].target,
        Some(aruna_core::jobs::JobTarget::Bucket {
            bucket: "target".to_string(),
            path: Some("crate".to_string()),
            exact: false,
        })
    );
    assert_eq!(listed[0].group_id, Some(Ulid::from_bytes([4u8; 16])));
    assert_eq!(listed[0].started_at_ms, 42);
    assert_eq!(listed[0].progress, None, "nothing counted yet");

    reporter.set_total(20);
    reporter.set_current(14);
    let progress = runtime.running_list()[0].progress.clone().unwrap();
    assert_eq!((progress.current, progress.total), (14, Some(20)));
}

#[derive(Default)]
struct RecordingReconciler {
    seen: std::sync::Mutex<Vec<JobId>>,
}

impl ExternalReconciler for RecordingReconciler {
    fn reconcile_lost_attempt<'a>(
        &'a self,
        _context: &'a DriverContext,
        record: JobRecord,
    ) -> futures_util::future::BoxFuture<'a, ()> {
        Box::pin(async move {
            self.seen.lock().unwrap().push(record.job_id);
        })
    }
}

// Internal and external slot accounting are independent: saturating one class
// leaves the other class fully available.
#[tokio::test]
async fn per_class_slots() {
    let runtime = JobsRuntime::with_capacity(2);
    for byte in 1..=2u8 {
        runtime
            .register_test_execution(JobId::from_bytes([byte; 16]), JobExecutionClass::InProcess);
    }
    assert_eq!(runtime.available_slots_for(JobExecutionClass::InProcess), 0);
    assert_eq!(
        runtime.available_slots_for(JobExecutionClass::ExternalAttempt),
        EXTERNAL_CONCURRENCY_CAP,
        "internal load never consumes external slots"
    );

    runtime.register_test_execution(
        JobId::from_bytes([9u8; 16]),
        JobExecutionClass::ExternalAttempt,
    );
    assert_eq!(
        runtime.available_slots_for(JobExecutionClass::ExternalAttempt),
        EXTERNAL_CONCURRENCY_CAP - 1
    );
    assert_eq!(
        runtime.available_slots_for(JobExecutionClass::InProcess),
        0,
        "external load never frees or consumes internal slots"
    );
}

#[test]
fn paused_runtime_starts() {
    let runtime = JobsRuntime::new_paused();
    assert!(!runtime.is_started());
    assert_eq!(runtime.available_slots_for(JobExecutionClass::InProcess), 0);
    assert_eq!(
        runtime.available_slots_for(JobExecutionClass::ExternalAttempt),
        0
    );

    runtime.start();

    assert!(runtime.is_started());
    assert_eq!(
        runtime.available_slots_for(JobExecutionClass::InProcess),
        JOB_CONCURRENCY_CAP
    );
    assert_eq!(
        runtime.available_slots_for(JobExecutionClass::ExternalAttempt),
        EXTERNAL_CONCURRENCY_CAP
    );
}

// Restart recovery routes an external attempt to the reconcile hook instead of
// re-queuing it, so a container that may still be running is never re-run.
#[tokio::test]
async fn recover_reconciles_external() {
    let (_dir, storage) = temp_storage();
    let context = aruna_operations::testing::test_context(storage.clone());
    let recorder = Arc::new(RecordingReconciler::default());
    let runtime = JobsRuntime::with_reconciler(recorder.clone());
    let job_id = JobId::from_bytes([0xE7; 16]);
    let mut record = probe_record(job_id, 3, 0, None);
    record.execution_class = JobExecutionClass::ExternalAttempt;
    record.state = JobState::Running;
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: Ulid::generate(),
        lease_expires_ms: unix_timestamp_millis() + 60_000,
    });
    record.attempt_intent = Some(AttemptIntent {
        attempt_no: 1,
        external_name: "attempt".to_string(),
        executor_kind: "docker".to_string(),
        pinned_image: "alpine@sha256:digest".to_string(),
        attempt_epoch: 1,
    });
    insert_job(&context, &record).await.unwrap();

    // No in-process job was recovered; the external one was reconciled, not requeued.
    assert_eq!(runtime.recover_stale_jobs(&context).await.unwrap(), 0);
    let after = read_job_record(&context, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.state, JobState::Running, "not requeued");
    assert_eq!(after.attempts, 0, "the restart spends no attempt");
    assert!(after.claim.is_some());
    assert_eq!(recorder.seen.lock().unwrap().as_slice(), &[job_id]);
}

fn external_running_record(job_id: JobId) -> JobRecord {
    let mut record = probe_record(job_id, 3, 0, None);
    record.execution_class = JobExecutionClass::ExternalAttempt;
    record.state = JobState::Running;
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: Ulid::generate(),
        lease_expires_ms: unix_timestamp_millis() + 60_000,
    });
    record.attempt_intent = Some(AttemptIntent {
        attempt_no: 1,
        external_name: "attempt".to_string(),
        executor_kind: "docker".to_string(),
        pinned_image: "alpine@sha256:digest".to_string(),
        attempt_epoch: 1,
    });
    record
}

#[derive(Default)]
struct BlockingReconciler {
    seen: std::sync::Mutex<Vec<JobId>>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl ExternalReconciler for BlockingReconciler {
    fn reconcile_lost_attempt<'a>(
        &'a self,
        _context: &'a DriverContext,
        record: JobRecord,
    ) -> futures_util::future::BoxFuture<'a, ()> {
        Box::pin(async move {
            self.seen.lock().unwrap().push(record.job_id);
            self.entered.notify_one();
            self.release.notified().await;
        })
    }
}

// An observed required-service failure while one recovery unit is pending
// lets that unit finish and admits no next one.
#[tokio::test]
async fn stop_between_units() {
    let (_dir, storage) = temp_storage();
    let context = aruna_operations::testing::test_context(storage.clone());
    let reconciler = Arc::new(BlockingReconciler::default());
    let runtime = JobsRuntime::with_reconciler(reconciler.clone());
    let first = JobId::from_bytes([0xE9; 16]);
    let second = JobId::from_bytes([0xEA; 16]);
    insert_job(&context, &external_running_record(first))
        .await
        .unwrap();
    insert_job(&context, &external_running_record(second))
        .await
        .unwrap();

    let stop = CancellationToken::new();
    let failure = std::sync::atomic::AtomicBool::new(false);
    let recover = runtime.recover_until_stopped(&context, &stop, || {
        failure.load(std::sync::atomic::Ordering::SeqCst)
    });
    tokio::pin!(recover);
    tokio::select! {
        _ = &mut recover => panic!("recovery must hold on the first recovery unit"),
        _ = reconciler.entered.notified() => {}
    }
    assert_eq!(reconciler.seen.lock().unwrap().len(), 1);

    // The failure arrives while the unit is still pending; releasing it
    // finishes that unit and the loop starts no next one.
    failure.store(true, std::sync::atomic::Ordering::SeqCst);
    reconciler.release.notify_one();
    assert_eq!(recover.await.unwrap(), 0);
    let processed = reconciler.seen.lock().unwrap().clone();
    assert_eq!(processed.len(), 1, "no next recovery unit after the stop");
    let unprocessed = if processed[0] == first { second } else { first };
    let untouched = read_job_record(&context, unprocessed, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(untouched.state, JobState::Running);
    assert!(untouched.claim.is_some());
}

// A stop accepted before recovery admits no recovery unit at all.
#[tokio::test]
async fn cancel_before_recovery() {
    let (_dir, storage) = temp_storage();
    let context = aruna_operations::testing::test_context(storage.clone());
    let recorder = Arc::new(RecordingReconciler::default());
    let runtime = JobsRuntime::with_reconciler(recorder.clone());
    let job_id = JobId::from_bytes([0xEB; 16]);
    insert_job(&context, &external_running_record(job_id))
        .await
        .unwrap();

    let stop = CancellationToken::new();
    stop.cancel();
    assert_eq!(
        runtime
            .recover_until_stopped(&context, &stop, || false)
            .await
            .unwrap(),
        0
    );
    assert!(recorder.seen.lock().unwrap().is_empty());
}

struct AdoptingReconciler;

impl ExternalReconciler for AdoptingReconciler {
    fn reconcile_lost_attempt<'a>(
        &'a self,
        context: &'a DriverContext,
        record: JobRecord,
    ) -> futures_util::future::BoxFuture<'a, ()> {
        Box::pin(async move {
            let outcome = adopt_external_attempt(
                context,
                record.job_id,
                record.owner_node_id,
                unix_timestamp_millis(),
            )
            .await
            .unwrap();
            assert!(matches!(outcome, AdoptOutcome::Adopted(..)), "must adopt");
        })
    }
}

// Rolling restarts must never exhaust JOB_MAX_ATTEMPTS on an external attempt whose
// container is still healthy and gets re-adopted every time.
#[tokio::test]
async fn recovery_spares_attempts() {
    let (_dir, storage) = temp_storage();
    let context = aruna_operations::testing::test_context(storage.clone());
    let runtime = JobsRuntime::with_reconciler(Arc::new(AdoptingReconciler));
    let job_id = JobId::from_bytes([0xE8; 16]);
    let token = Ulid::generate();
    let mut record = probe_record(job_id, 3, 0, None);
    record.execution_class = JobExecutionClass::ExternalAttempt;
    record.state = JobState::Running;
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: token,
        lease_expires_ms: unix_timestamp_millis() + 60_000,
    });
    insert_job(&context, &record).await.unwrap();
    record_attempt_intent(
        &context,
        job_id,
        token,
        AttemptIntent {
            attempt_no: 1,
            external_name: "attempt".to_string(),
            executor_kind: "docker".to_string(),
            pinned_image: "alpine@sha256:digest".to_string(),
            attempt_epoch: 0,
        },
        None,
        None,
        unix_timestamp_millis(),
    )
    .await
    .unwrap();

    for restart in 0..(JOB_MAX_ATTEMPTS + 2) {
        expire_lease(&context, job_id).await;
        assert_eq!(runtime.recover_stale_jobs(&context).await.unwrap(), 0);
        let after = read_job_record(&context, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.state, JobState::Running, "restart {restart}");
        assert_eq!(after.attempts, 0, "restart {restart}");
        assert!(after.claim.is_some(), "restart {restart}");
    }
}

async fn expire_lease(context: &DriverContext, job_id: JobId) {
    mutate_job(context, job_id, |record| {
        if let Some(claim) = record.claim.as_mut() {
            claim.lease_expires_ms = 1;
        }
        Ok(JobMutation::Persist)
    })
    .await
    .unwrap();
}

/// Cancels a job of `user_id` and signals its running executions, as the cancel route does.
async fn cancel_owned_job(
    context: &DriverContext,
    runtime: &JobsRuntime,
    user_id: UserId,
    job_id: JobId,
) -> Result<aruna_operations::jobs::job_cancel::CancelJobOutcome, String> {
    let now_ms = unix_timestamp_millis();
    let cancel =
        aruna_operations::jobs::job_cancel::CancelOwnedOperation::new(user_id, job_id, now_ms);
    let report = drive(cancel, context).await.map_err(|error| error.0)?;
    for running in report.running {
        runtime.request_cancel(running);
    }
    Ok(report.outcome)
}
