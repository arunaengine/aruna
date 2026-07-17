use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aruna_core::structs::{JobId, JobPayload, JobRecord, JobState, RealmId};
use aruna_core::types::{NodeId, UserId};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::drain::{JobClassBudget, process_job_queue_batch};
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::jobs::store::{claim_job, insert_job, read_job_record, set_cancel_requested};
use aruna_storage::{FjallPersistPolicy, FjallStorage, StorageHandle};
use aruna_tasks::TaskHandle;
use ulid::Ulid;

const CHILD_MODE_ENV: &str = "ARUNA_JOB_RESTART_CHILD";
const CHILD_STORAGE_ENV: &str = "ARUNA_JOB_RESTART_STORAGE";
const CHILD_MARKER_A_ENV: &str = "ARUNA_JOB_RESTART_MARKER_A";
const CHILD_MARKER_B_ENV: &str = "ARUNA_JOB_RESTART_MARKER_B";
const CHILD_TEST_NAME: &str = "restart_recovery_child";

fn node_id(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn owner() -> UserId {
    UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]))
}

fn job_a() -> JobId {
    JobId::from_bytes([0xA1; 16])
}

fn job_b() -> JobId {
    JobId::from_bytes([0xB2; 16])
}

fn probe(job_id: JobId, steps: u32, sleep_ms: u64, marker: &str) -> JobRecord {
    JobRecord::new(
        job_id,
        JobPayload::Probe {
            steps,
            step_sleep_ms: sleep_ms,
            fail_at: None,
            panic_at: None,
            cleanup_marker: Some(marker.to_string()),
        },
        owner(),
        node_id(1),
        1,
        1,
        None,
    )
}

#[tokio::test]
async fn restart_recovers_jobs() -> Result<(), Box<dyn std::error::Error>> {
    let storage_dir = tempfile::tempdir()?;
    let marker_dir = tempfile::tempdir()?;
    let marker_a = marker_dir.path().join("job-a");
    let marker_b = marker_dir.path().join("job-b");

    let output = Command::new(env::current_exe()?)
        .arg("--ignored")
        .arg("--exact")
        .arg(CHILD_TEST_NAME)
        .arg("--nocapture")
        .env(CHILD_MODE_ENV, "1")
        .env(CHILD_STORAGE_ENV, storage_dir.path())
        .env(CHILD_MARKER_A_ENV, &marker_a)
        .env(CHILD_MARKER_B_ENV, &marker_b)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "job restart child failed with {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    assert!(marker_b.exists(), "child should leave partial state behind");

    let storage = FjallStorage::open(storage_dir.path().to_str().unwrap())?;
    let runtime = JobsRuntime::new();
    let recovered = runtime.recover_stale_jobs(&storage).await?;
    assert_eq!(recovered, 2, "both in-flight jobs are re-queued at startup");

    let context = Arc::new(DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });

    drive_until_terminal(&context, &runtime).await?;

    let a = read_job_record(&storage, job_a(), None).await?.unwrap();
    let b = read_job_record(&storage, job_b(), None).await?.unwrap();
    assert_eq!(
        a.state,
        JobState::Succeeded,
        "normal job re-drives to success"
    );
    assert_eq!(
        b.state,
        JobState::Cancelled,
        "cancel-requested job is cancelled"
    );
    assert!(!marker_b.exists(), "cleanup runs on the recovered cancel");
    Ok(())
}

async fn drive_until_terminal(
    context: &Arc<DriverContext>,
    runtime: &Arc<JobsRuntime>,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let batch = process_job_queue_batch(
            &context.storage_handle,
            node_id(9),
            JobClassBudget {
                in_process: 8,
                external: 8,
            },
            None,
        )
        .await?;
        for record in batch.claimed {
            runtime.spawn(context.clone(), record);
        }
        let a = read_job_record(&context.storage_handle, job_a(), None).await?;
        let b = read_job_record(&context.storage_handle, job_b(), None).await?;
        let done = a.is_some_and(|record| record.state.is_terminal())
            && b.is_some_and(|record| record.state.is_terminal());
        if done {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err("jobs did not reach terminal state after restart".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Simulates a crash mid-flight: claims two jobs, flags one for cancellation, leaves
/// partial-state markers, flushes, and exits without running destructors.
#[tokio::test]
#[ignore = "spawned by restart_recovers_jobs"]
async fn restart_recovery_child() -> Result<(), Box<dyn std::error::Error>> {
    if env::var(CHILD_MODE_ENV).ok().as_deref() != Some("1") {
        return Ok(());
    }
    let storage_path = child_env(CHILD_STORAGE_ENV)?;
    let marker_a = child_env(CHILD_MARKER_A_ENV)?;
    let marker_b = child_env(CHILD_MARKER_B_ENV)?;

    let storage = FjallStorage::open_with_persist_policy(
        storage_path.to_str().ok_or("invalid storage path")?,
        FjallPersistPolicy::SyncAll,
    )?;

    seed_claimed(&storage, probe(job_a(), 3, 0, marker_a.to_str().unwrap())).await;
    std::fs::write(&marker_a, b"partial")?;

    seed_claimed(
        &storage,
        probe(job_b(), 100, 10, marker_b.to_str().unwrap()),
    )
    .await;
    set_cancel_requested(&storage, job_b(), 2).await.unwrap();
    std::fs::write(&marker_b, b"partial")?;

    storage.sync_all().await?;
    std::process::exit(0);
}

async fn seed_claimed(storage: &StorageHandle, mut record: JobRecord) {
    record.has_run = true;
    insert_job(storage, &record).await.unwrap();
    claim_job(storage, record.job_id, node_id(1), 1)
        .await
        .unwrap();
}

fn child_env(name: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| format!("missing {name}").into())
}
